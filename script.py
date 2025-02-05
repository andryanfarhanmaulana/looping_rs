import asyncio
import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from web3 import Web3
from web3.contract import Contract
from web3.exceptions import BlockNotFound
from web3.providers.async_http_provider import AsyncHTTPProvider
from web3.types import LogReceipt, BlockData

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# В реальном приложении эти данные должны загружаться из защищенного хранилища
# или переменных окружения, а не храниться в коде.

@dataclass
class ChainConfig:
    name: str
    rpc_url: str
    bridge_contract_address: str
    confirmation_blocks: int = 3

CONFIG = {
    "source_chain": ChainConfig(
        name="Ethereum",
        rpc_url="https://rpc.ankr.com/eth", # Используем публичный RPC Ankr
        bridge_contract_address="0x9999999999999999999999999999999999999999", # Placeholder
        confirmation_blocks=5,
    ),
    "target_chain": ChainConfig(
        name="Polygon",
        rpc_url="https://rpc.ankr.com/polygon", # Используем публичный RPC Ankr
        bridge_contract_address="0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", # Placeholder
        confirmation_blocks=10,
    ),
    "poll_interval_seconds": 10,
}

# Упрощенные ABI для событий моста. В реальном проекте здесь будет полный ABI контракта.
DUMMY_BRIDGE_ABI = [ 
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "bytes32", "name": "transactionId", "type": "bytes32"},
            {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256"},
            {"indexed": False, "internalType": "string", "name": "targetChain", "type": "string"}
        ],
        "name": "TokensLocked",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "bytes32", "name": "transactionId", "type": "bytes32"},
            {"indexed": True, "internalType": "address", "name": "recipient", "type": "address"}
        ],
        "name": "TokensUnlocked",
        "type": "event"
    }
]

# ==============================================================================
# LOGGING SETUP
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s:%(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==============================================================================
# CUSTOM EXCEPTIONS
# ==============================================================================

class ConnectionError(Exception):
    "Исключение при ошибке подключения к RPC ноде."
    pass

class ConfigurationError(Exception):
    "Исключение при некорректной конфигурации."
    pass

# ==============================================================================
# CORE ARCHITECTURE CLASSES
# ==============================================================================

@dataclass
class CrossChainTransaction:
    "Структура для хранения данных о кросс-чейн транзакции."
    tx_id: str
    status: str = "PENDING" # PENDING, COMPLETED, FAILED
    source_chain: str = ""
    target_chain: str = ""
    user: str = ""
    amount: float = 0.0
    lock_event_details: Dict[str, Any] = field(default_factory=dict)
    unlock_event_details: Optional[Dict[str, Any]] = None

class EventProcessor:
    "Централизованный обработчик событий, управляющий состоянием транзакций."

    def __init__(self):
        # В реальной системе здесь была бы база данных (например, Redis или PostgreSQL).
        # Для симуляции используем словарь в памяти.
        self.transactions: Dict[str, CrossChainTransaction] = {}
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)

    async def process_lock_event(self, event: LogReceipt, chain_name: str):
        "Обрабатывает событие 'TokensLocked' с исходной цепи."
        args = event['args']
        tx_id = args['transactionId'].hex()

        async with self._lock:
            if tx_id in self.transactions:
                self.logger.warning(f"Дублирующееся событие Lock для tx_id: {tx_id}. Игнорируем.")
                return

            # Предполагаем, что amount в 'wei', конвертируем в 'ether' для читаемости
            amount_readable = args['amount'] / (10**18)

            tx = CrossChainTransaction(
                tx_id=tx_id,
                source_chain=chain_name,
                target_chain=args['targetChain'],
                user=args['sender'],
                amount=amount_readable,
                lock_event_details={
                    'blockNumber': event['blockNumber'],
                    'transactionHash': event['transactionHash'].hex(),
                }
            )
            self.transactions[tx_id] = tx
            self.logger.info(
                f"РЕГИСТРАЦИЯ БЛОКИРОВКИ: TxID [{tx_id[:10]}...] | Пользователь [{tx.user}] | "
                f"Сумма [{tx.amount:.4f}] | Из [{tx.source_chain}] -> В [{tx.target_chain}]. Статус: PENDING."
            )

    async def process_unlock_event(self, event: LogReceipt):
        "Обрабатывает событие 'TokensUnlocked' с целевой цепи."
        args = event['args']
        tx_id = args['transactionId'].hex()

        async with self._lock:
            if tx_id not in self.transactions:
                self.logger.warning(
                    f"Получено событие Unlock для неизвестной транзакции (tx_id: {tx_id}). "
                    f"Возможно, событие Lock еще не обработано или произошло ранее."
                )
                return
            
            tx = self.transactions[tx_id]
            if tx.status == "COMPLETED":
                self.logger.warning(f"Дублирующееся событие Unlock для tx_id: {tx_id}. Игнорируем.")
                return

            tx.status = "COMPLETED"
            tx.unlock_event_details = {
                'blockNumber': event['blockNumber'],
                'transactionHash': event['transactionHash'].hex(),
            }
            self.logger.info(
                f"ПОДТВЕРЖДЕНИЕ РАЗБЛОКИРОВКИ: TxID [{tx_id[:10]}...] | Статус изменен на COMPLETED."
            )

    def log_state_summary(self):
        "Выводит сводку по текущему состоянию транзакций."
        pending = sum(1 for tx in self.transactions.values() if tx.status == 'PENDING')
        completed = sum(1 for tx in self.transactions.values() if tx.status == 'COMPLETED')
        self.logger.info(f"Сводка состояния: PENDING: {pending}, COMPLETED: {completed}")


class EVMChainConnector:
    "Обеспечивает подключение к EVM-совместимой сети через Web3.py."

    def __init__(self, config: ChainConfig):
        self.config = config
        self.logger = logging.getLogger(f"{self.config.name}Connector")
        self.web3: Optional[Web3] = None
        self.bridge_contract: Optional[Contract] = None

    async def connect(self):
        "Устанавливает асинхронное соединение с RPC-узлом."
        self.logger.info(f"Подключение к {self.config.name} через {self.config.rpc_url}...")
        try:
            provider = AsyncHTTPProvider(self.config.rpc_url)
            self.web3 = Web3(provider)
            if not await self.web3.is_connected():
                raise ConnectionError(f"Не удалось подключиться к {self.config.name}.")
            
            self.bridge_contract = self.web3.eth.contract(
                address=Web3.to_checksum_address(self.config.bridge_contract_address),
                abi=DUMMY_BRIDGE_ABI
            )
            self.logger.info(f"Успешное подключение к {self.config.name}. ChainID: {await self.web3.eth.chain_id}")
        except Exception as e:
            self.logger.error(f"Ошибка при подключении к {self.config.name}: {e}")
            raise ConnectionError from e

    async def get_latest_block_number(self) -> int:
        "Возвращает номер последнего блока."
        if not self.web3:
            raise ConnectionError("Соединение не установлено.")
        return await self.web3.eth.block_number


class ChainEventListener:
    "Слушатель событий для одной блокчейн-цепочки."

    def __init__(self, chain_config: ChainConfig, event_processor: EventProcessor, is_source_chain: bool):
        self.config = chain_config
        self.connector = EVMChainConnector(chain_config)
        self.processor = event_processor
        self.is_source_chain = is_source_chain
        self.logger = logging.getLogger(f"{self.config.name}Listener")
        self._last_processed_block = 0

    async def _initialize_start_block(self):
        "Инициализирует начальный блок для сканирования."
        try:
            latest_block = await self.connector.get_latest_block_number()
            # Начинаем сканирование с небольшим отставанием, чтобы избежать проблем с reorg
            self._last_processed_block = latest_block - self.config.confirmation_blocks
            self.logger.info(
                f"Инициализация слушателя для {self.config.name}. "
                f"Начальный блок: {self._last_processed_block} (Последний: {latest_block})"
            )
        except ConnectionError as e:
            self.logger.error(f"Не удалось получить начальный блок для {self.config.name}: {e}")
            raise

    async def _scan_block_range(self, from_block: int, to_block: int):
        "Сканирует диапазон блоков на наличие целевых событий."
        if not self.connector.web3 or not self.connector.bridge_contract:
            self.logger.error("Коннектор или контракт не инициализирован.")
            return

        event_name = 'TokensLocked' if self.is_source_chain else 'TokensUnlocked'
        event_filter = self.connector.bridge_contract.events[event_name].create_filter(
            fromBlock=from_block,
            toBlock=to_block
        )

        try:
            events = await event_filter.get_all_entries()
            if events:
                self.logger.info(f"Найдено {len(events)} '{event_name}' событий в блоках {from_block}-{to_block}.")
                for event in events:
                    if self.is_source_chain:
                        await self.processor.process_lock_event(event, self.config.name)
                    else:
                        await self.processor.process_unlock_event(event)
        except BlockNotFound:
             self.logger.warning(f"Блоки в диапазоне {from_block}-{to_block} не найдены. Возможно, произошел reorg.")
             # В этом случае мы можем откатить _last_processed_block, но для симуляции пропустим
        except Exception as e:
            self.logger.error(f"Ошибка при сканировании событий в блоках {from_block}-{to_block}: {e}")

    async def run(self):
        "Основной цикл работы слушателя."
        await self.connector.connect()
        await self._initialize_start_block()

        while True:
            try:
                latest_block = await self.connector.get_latest_block_number()
                
                # Определяем конечный блок для сканирования с учетом подтверждений
                to_block = latest_block - self.config.confirmation_blocks

                if to_block > self._last_processed_block:
                    from_block = self._last_processed_block + 1
                    self.logger.debug(f"Сканируем блоки с {from_block} по {to_block} на {self.config.name}")
                    await self._scan_block_range(from_block, to_block)
                    self._last_processed_block = to_block
                else:
                    self.logger.debug(f"Нет новых подтвержденных блоков на {self.config.name}. Последний: {self._last_processed_block}")
                
                await asyncio.sleep(CONFIG["poll_interval_seconds"])

            except ConnectionError as e:
                self.logger.error(f"Ошибка соединения с {self.config.name}: {e}. Попытка переподключения через 30 секунд.")
                await asyncio.sleep(30)
                try:
                    await self.connector.connect()
                except ConnectionError:
                    self.logger.error("Переподключение не удалось.")
            except Exception as e:
                self.logger.critical(f"Критическая ошибка в цикле слушателя {self.config.name}: {e}", exc_info=True)
                await asyncio.sleep(60) # Пауза перед следующей попыткой


class CrossChainOrchestrator:
    "Оркестратор, который запускает и координирует слушателей для обеих сетей."

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.event_processor = EventProcessor()
        
        self.source_chain_listener = ChainEventListener(
            chain_config=CONFIG["source_chain"],
            event_processor=self.event_processor,
            is_source_chain=True
        )
        self.target_chain_listener = ChainEventListener(
            chain_config=CONFIG["target_chain"],
            event_processor=self.event_processor,
            is_source_chain=False
        )
        self.tasks: List[asyncio.Task] = []

    async def _run_state_logger(self):
        "Периодически выводит сводку о состоянии транзакций."
        while True:
            await asyncio.sleep(60)
            self.event_processor.log_state_summary()
    
    async def _simulate_events(self):
        "Симулирует возникновение событий для демонстрации работы."
        await asyncio.sleep(20) # Начальная задержка
        self.logger.info("--- СИМУЛЯЦИЯ: Начало генерации событий ---")
        while True:
            await asyncio.sleep(random.uniform(15, 45))
            # Симулируем событие блокировки
            tx_id = '0x' + random.randbytes(32).hex()
            mock_lock_event = {
                'args': {
                    'transactionId': bytes.fromhex(tx_id[2:]),
                    'sender': '0x' + random.randbytes(20).hex(),
                    'amount': random.randint(1, 1000) * (10**18),
                    'targetChain': self.target_chain_listener.config.name
                },
                'blockNumber': 1, # Placeholder
                'transactionHash': ('0x' + random.randbytes(32).hex()).encode()
            }
            await self.event_processor.process_lock_event(mock_lock_event, self.source_chain_listener.config.name)

            # Симулируем событие разблокировки с задержкой
            if random.random() > 0.1: # 90% шанс на успешную разблокировку
                await asyncio.sleep(random.uniform(5, 20))
                mock_unlock_event = {
                    'args': {
                        'transactionId': bytes.fromhex(tx_id[2:]),
                        'recipient': '0x' + random.randbytes(20).hex()
                    },
                    'blockNumber': 2, # Placeholder
                    'transactionHash': ('0x' + random.randbytes(32).hex()).encode()
                }
                await self.event_processor.process_unlock_event(mock_unlock_event)
            else:
                 self.logger.warning(f"СИМУЛЯЦИЯ: Транзакция {tx_id[:10]}... 'зависла' и не была разблокирована.")

    async def start(self):
        "Запускает все компоненты системы."
        self.logger.info("Запуск оркестратора Cross-Chain Bridge Listener...")
        try:
            self.tasks.append(asyncio.create_task(self.source_chain_listener.run()))
            self.tasks.append(asyncio.create_task(self.target_chain_listener.run()))
            self.tasks.append(asyncio.create_task(self._run_state_logger()))
            self.tasks.append(asyncio.create_task(self._simulate_events()))

            await asyncio.gather(*self.tasks)

        except asyncio.CancelledError:
            self.logger.info("Получен сигнал отмены. Завершение работы...")
        finally:
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.logger.info("Оркестратор успешно остановлен.")


def main():
    orchestrator = CrossChainOrchestrator()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(orchestrator.start())
    except KeyboardInterrupt:
        print("\nПолучен сигнал KeyboardInterrupt. Начинаю graceful shutdown...")
        # Graceful shutdown инициируется через отмену основной задачи
        tasks = asyncio.all_tasks(loop=loop)
        for t in tasks:
            t.cancel()
        
        # Даем время на завершение задач
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    finally:
        loop.close()
        logging.info("Цикл событий завершен.")

if __name__ == "__main__":
    main()
