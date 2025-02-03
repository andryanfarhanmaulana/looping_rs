import asyncio
import hashlib
import json
import logging
import random
import time
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field

# Cryptography for signing and verification, essential for blockchain security
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import InvalidSignature

# Faker for generating more realistic-looking test data
from faker import Faker

# --- Simulation Configuration ---
SIMULATION_CONFIG = {
    "NUM_VALIDATORS": 4,
    "BLOCK_TIME_SECONDS": 5,  # Time slot for each block proposer
    "TRANSACTIONS_PER_SECOND": 2,
    "MAX_TX_PER_BLOCK": 10,
    "SIMULATION_DURATION_BLOCKS": 20,
    "NETWORK_LATENCY_MS": (50, 200) # Min/Max simulated network latency
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("PoS_Simulator")
fake = Faker()

# --- Cryptography Utilities ---

def generate_key_pair() -> Tuple[ec.EllipticCurvePrivateKey, ec.EllipticCurvePublicKey]:
    """Generates an ECDSA private and public key pair."""
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()
    return private_key, public_key

def get_address_from_public_key(public_key: ec.EllipticCurvePublicKey) -> str:
    """Creates a human-readable address from a public key (simplified)."""
    key_bytes = public_key.public_numbers().x.to_bytes(32, 'big') + \
                public_key.public_numbers().y.to_bytes(32, 'big')
    return '0x' + hashlib.sha256(key_bytes).hexdigest()[-40:]

# --- Core Data Structures ---

@dataclass
class Transaction:
    """Represents a single transaction in the system."""
    sender: str
    receiver: str
    amount: float
    nonce: int
    signature: Optional[str] = None

    def to_dict_for_signing(self) -> Dict[str, Any]:
        """Returns the transaction data as a dict for signing (without the signature)."""
        return {"sender": self.sender, "receiver": self.receiver, "amount": self.amount, "nonce": self.nonce}

    def get_hash(self) -> bytes:
        """Calculates the hash of the transaction data."""
        tx_string = json.dumps(self.to_dict_for_signing(), sort_keys=True).encode('utf-8')
        return hashlib.sha256(tx_string).digest()

    def sign(self, private_key: ec.EllipticCurvePrivateKey):
        """Signs the transaction with the sender's private key."""
        signature_bytes = private_key.sign(self.get_hash(), ec.ECDSA(hashes.SHA256()))
        self.signature = signature_bytes.hex()

    def is_valid(self, public_key: ec.EllipticCurvePublicKey) -> bool:
        """Verifies the transaction's signature."""
        if not self.signature:
            return False
        try:
            signature_bytes = bytes.fromhex(self.signature)
            public_key.verify(signature_bytes, self.get_hash(), ec.ECDSA(hashes.SHA256()))
            return True
        except (InvalidSignature, ValueError):
            return False

@dataclass
class Block:
    """Represents a block in the blockchain."""
    height: int
    timestamp: float
    transactions: List[Transaction]
    previous_hash: str
    validator_address: str
    signature: Optional[str] = None

    def to_dict_for_signing(self) -> Dict[str, Any]:
        """Returns the block data as a dict for signing (without the signature)."""
        return {
            "height": self.height,
            "timestamp": self.timestamp,
            "transactions": [tx.__dict__ for tx in self.transactions],
            "previous_hash": self.previous_hash,
            "validator_address": self.validator_address
        }

    def calculate_hash(self) -> str:
        """Calculates the SHA256 hash of the block."""
        block_string = json.dumps(self.to_dict_for_signing(), sort_keys=True).encode('utf-8')
        return hashlib.sha256(block_string).hexdigest()

    def sign(self, private_key: ec.EllipticCurvePrivateKey):
        """Signs the block hash with the validator's private key."""
        block_hash_bytes = bytes.fromhex(self.calculate_hash())
        signature_bytes = private_key.sign(block_hash_bytes, ec.ECDSA(hashes.SHA256()))
        self.signature = signature_bytes.hex()

# --- Core System Components ---

class Mempool:
    """Manages a pool of unconfirmed transactions for a validator."""
    def __init__(self):
        self.pending_transactions: Dict[str, Transaction] = {}

    def add_transaction(self, transaction: Transaction) -> bool:
        """Adds a transaction to the mempool if it's not already present."""
        tx_hash = transaction.get_hash().hex()
        if tx_hash not in self.pending_transactions:
            self.pending_transactions[tx_hash] = transaction
            return True
        return False

    def get_transactions_for_block(self, max_tx: int) -> List[Transaction]:
        """Retrieves a batch of transactions to be included in a new block."""
        # Simple FIFO selection
        tx_hashes = list(self.pending_transactions.keys())
        selected_hashes = tx_hashes[:max_tx]
        return [self.pending_transactions[h] for h in selected_hashes]

    def remove_transactions(self, transactions: List[Transaction]):
        """Removes transactions that have been included in a block."""
        for tx in transactions:
            tx_hash = tx.get_hash().hex()
            if tx_hash in self.pending_transactions:
                del self.pending_transactions[tx_hash]

class Blockchain:
    """Manages the state of the blockchain for a single validator."""
    def __init__(self, genesis_block: Block):
        self.chain: List[Block] = [genesis_block]

    def get_latest_block(self) -> Block:
        """Returns the most recent block in the chain."""
        return self.chain[-1]

    def add_block(self, block: Block) -> bool:
        """Adds a new block to the chain after validation."""
        latest_block = self.get_latest_block()
        # Basic validation: height and previous hash
        if block.height != latest_block.height + 1:
            logger.warning(f"Invalid block height: expected {latest_block.height + 1}, got {block.height}")
            return False
        if block.previous_hash != latest_block.calculate_hash():
            logger.warning(f"Invalid previous hash for block {block.height}")
            return False
        
        self.chain.append(block)
        return True

class P2PNetworkSimulator:
    """Simulates a basic P2P network for broadcasting events."""
    def __init__(self):
        self.nodes: List['Validator'] = []

    def register_node(self, node: 'Validator'):
        """Adds a new validator node to the network."""
        self.nodes.append(node)

    async def broadcast_block(self, block: Block, origin_node_address: str):
        """Broadcasts a new block to all other nodes with simulated latency."""
        logger.info(f"Node {origin_node_address[:10]}... broadcasting block {block.height}")
        for node in self.nodes:
            if node.address != origin_node_address:
                latency = random.uniform(*SIMULATION_CONFIG['NETWORK_LATENCY_MS']) / 1000.0
                await asyncio.sleep(latency)
                await node.event_queue.put(("new_block", block))
    
    async def broadcast_transaction(self, tx: Transaction):
        """Broadcasts a new transaction to all nodes."""
        for node in self.nodes:
            latency = random.uniform(*SIMULATION_CONFIG['NETWORK_LATENCY_MS']) / 1000.0
            await asyncio.sleep(latency)
            await node.event_queue.put(("new_transaction", tx))

class Validator:
    """Represents a validator node in the PoS network."""
    def __init__(self, network: P2PNetworkSimulator, genesis_block: Block):
        self.private_key, self.public_key = generate_key_pair()
        self.address = get_address_from_public_key(self.public_key)
        self.mempool = Mempool()
        self.blockchain = Blockchain(genesis_block)
        self.network = network
        self.event_queue = asyncio.Queue()
        self.logger = logging.getLogger(f"Validator-{self.address[:10]}")
        self.logger.info(f"Initialized with address {self.address}")

    def _is_my_turn(self, current_proposer_index: int, all_validators: List['Validator']) -> bool:
        """Checks if it is this validator's turn to propose a block (Round-Robin)."""
        return all_validators[current_proposer_index] == self

    def create_block(self) -> Optional[Block]:
        """Creates a new block from transactions in the mempool."""
        latest_block = self.blockchain.get_latest_block()
        transactions_to_include = self.mempool.get_transactions_for_block(SIMULATION_CONFIG['MAX_TX_PER_BLOCK'])
        
        # Edge case: Don't create an empty block if no transactions are available
        # In a real network, empty blocks might be allowed to maintain block time.
        if not transactions_to_include:
            self.logger.info("Mempool is empty, skipping block creation.")
            return None

        new_block = Block(
            height=latest_block.height + 1,
            timestamp=time.time(),
            transactions=transactions_to_include,
            previous_hash=latest_block.calculate_hash(),
            validator_address=self.address
        )
        new_block.sign(self.private_key)
        self.logger.info(f"Created and signed new block {new_block.height} with {len(new_block.transactions)} transactions.")
        return new_block

    def validate_and_process_block(self, block: Block, proposer_public_key: ec.EllipticCurvePublicKey) -> bool:
        """Performs a full validation of a received block."""
        # 1. Verify the block's own signature
        calculated_hash_bytes = bytes.fromhex(block.calculate_hash())
        try:
            signature_bytes = bytes.fromhex(block.signature)
            proposer_public_key.verify(signature_bytes, calculated_hash_bytes, ec.ECDSA(hashes.SHA256()))
        except (InvalidSignature, ValueError):
            self.logger.warning(f"Block {block.height} has an invalid signature from validator {block.validator_address[:10]}...")
            return False
        
        # 2. Add block to local chain (checks height and prev_hash)
        if not self.blockchain.add_block(block):
            self.logger.warning(f"Failed to add block {block.height} to local chain.")
            return False
        
        # 3. If successfully added, remove its transactions from our mempool
        self.mempool.remove_transactions(block.transactions)
        self.logger.info(f"Validated and added block {block.height}. Chain height is now {self.blockchain.get_latest_block().height}.")
        return True

    async def process_events(self, all_validators: List['Validator']):
        """The main loop for processing events from the queue."""
        while True:
            event_type, data = await self.event_queue.get()
            if event_type == "new_transaction":
                if self.mempool.add_transaction(data):
                    self.logger.debug(f"Added new tx to mempool. Current size: {len(self.mempool.pending_transactions)}")
            
            elif event_type == "new_block":
                block: Block = data
                proposer_address = block.validator_address
                proposer = next((v for v in all_validators if v.address == proposer_address), None)
                if not proposer:
                    self.logger.warning(f"Received block from unknown validator {proposer_address}")
                    continue
                
                # Avoid processing our own block broadcast back to us
                if self.address == proposer_address:
                    continue
                
                self.logger.info(f"Received block {block.height} from {proposer_address[:10]}...")
                self.validate_and_process_block(block, proposer.public_key)
            
            self.event_queue.task_done()

# --- Simulation Driver ---

async def transaction_generator(network: P2PNetworkSimulator, validators: List[Validator]):
    """A coroutine that continuously generates and broadcasts transactions."""
    nonce_map = {v.address: 0 for v in validators}
    while True:
        sender_validator = random.choice(validators)
        receiver_validator = random.choice([v for v in validators if v != sender_validator])
        
        tx = Transaction(
            sender=sender_validator.address,
            receiver=receiver_validator.address,
            amount=round(random.uniform(0.1, 10.0), 4),
            nonce=nonce_map[sender_validator.address]
        )
        tx.sign(sender_validator.private_key)
        nonce_map[sender_validator.address] += 1

        await network.broadcast_transaction(tx)
        
        await asyncio.sleep(1.0 / SIMULATION_CONFIG['TRANSACTIONS_PER_SECOND'])


async def main():
    """Main function to set up and run the PoS simulation."""
    logger.info("--- Starting Proof-of-Stake Validator Simulation ---")
    
    # 1. Setup network and genesis block
    network = P2PNetworkSimulator()
    genesis_block = Block(
        height=0,
        timestamp=time.time(),
        transactions=[],
        previous_hash="0"*64,
        validator_address="SYSTEM_GENESIS"
    )
    
    # 2. Create validators and register them with the network
    validators = [Validator(network, genesis_block) for _ in range(SIMULATION_CONFIG['NUM_VALIDATORS'])]
    for v in validators:
        network.register_node(v)
    
    # 3. Start event processing and transaction generation tasks for each validator
    for v in validators:
        asyncio.create_task(v.process_events(validators))
    
    asyncio.create_task(transaction_generator(network, validators))
    
    # 4. Main simulation loop (block production cycle)
    try:
        for block_height in range(1, SIMULATION_CONFIG['SIMULATION_DURATION_BLOCKS'] + 1):
            # Determine proposer using Round-Robin for simplicity
            proposer_index = (block_height - 1) % len(validators)
            proposer = validators[proposer_index]
            
            logger.info(f"\n--- Block Slot {block_height} | Proposer: {proposer.address[:10]}... ---")
            
            # Wait for block time
            await asyncio.sleep(SIMULATION_CONFIG['BLOCK_TIME_SECONDS'])
            
            # Proposer creates and broadcasts a block
            new_block = proposer.create_block()
            if new_block:
                # In a real network, the proposer would also add it to its own chain
                proposer.blockchain.add_block(new_block)
                proposer.mempool.remove_transactions(new_block.transactions)
                
                await network.broadcast_block(new_block, proposer.address)
            else:
                 logger.info(f"Proposer {proposer.address[:10]}... did not create a block.")

    except asyncio.CancelledError:
        logger.info("Simulation cancelled.")
    finally:
        logger.info("--- Simulation Finished ---")
        # Final check of chain states
        for v in validators:
            logger.info(f"Validator {v.address[:10]}... ended with chain height: {v.blockchain.get_latest_block().height}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nSimulation stopped by user.")

