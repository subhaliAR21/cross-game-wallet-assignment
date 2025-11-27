import threading
import time
import random

# --- Core Wallet Service Implementation ---

class WalletService:
    """
    An in-memory backend for managing user wallets, ensuring atomic updates 
    and request idempotency.
    """
    def __init__(self):
        # Stores user wallet data: {userId: {'balance': int, 'operations': list}}
        self.wallets = {}
        
        # Stores completed requests for idempotency: 
        # {idempotencyKey: {'status': 'completed', 'result': 'Success', 'timestamp': float}}
        self.idempotency_map = {}
        
        # Lock to ensure atomic updates to shared memory (self.wallets and self.idempotency_map)
        # This is the solution to the "concurrent modifications" constraint.
        self.lock = threading.Lock()
        
        print("WalletService Initialized.")

    # Helper function to get or initialize a user's wallet
    def _get_or_create_wallet(self, user_id):
        if user_id not in self.wallets:
            self.wallets[user_id] = {'balance': 0, 'operations': []}
        return self.wallets[user_id]

    # --- API Implementations ---

    def topup(self, user_id: str, amount_usd: float, idempotency_key: str):
        """
        POST /wallet/topup: Converts USD to Coins (1:1) and credits wallet.
        """
        amount_coins = int(amount_usd) # 1 USD = 1 Kraft Coin
        
        # Start of Critical Section: We acquire the lock here.
        with self.lock:
            
            # 1. Idempotency Check (Must happen inside the lock)
            if idempotency_key in self.idempotency_map:
                print(f"[{user_id}] 🔑 Topup (ID: {idempotency_key}) is idempotent. Returning previous success.")
                return self.idempotency_map[idempotency_key]['result']

            # 2. Atomic Balance Update (Read-Modify-Write)
            wallet = self._get_or_create_wallet(user_id)
            
            # Simulate a slow operation to make the race condition more likely if the lock was missing
            time.sleep(random.uniform(0.01, 0.05)) 
            
            # Modification: This is the critical step for atomicity
            new_balance = wallet['balance'] + amount_coins
            wallet['balance'] = new_balance
            
            operation = {
                'type': 'topup',
                'amount': amount_coins,
                'key': idempotency_key,
                'time': time.time()
            }
            wallet['operations'].append(operation)
            
            # 3. Record successful operation for future idempotency checks
            result_message = f"Topup successful. Added {amount_coins} coins. New balance: {new_balance}"
            self.idempotency_map[idempotency_key] = {
                'status': 'completed',
                'result': result_message,
                'timestamp': time.time()
            }
            print(f"[{user_id}] ✅ Topup processed. Amount: {amount_coins}. New Balance: {new_balance}")
            
            # Lock is automatically released here (End of Critical Section)
            return result_message

    def game_reward(self, user_id: str, amount_coins: int, reward_id: str, idempotency_key: str):
        """
        POST /game/reward: Credits reward coins to wallet.
        """
        
        # Start of Critical Section: Acquire the lock.
        with self.lock:
            if idempotency_key in self.idempotency_map:
                print(f"[{user_id}] 🔑 Reward (ID: {idempotency_key}) is idempotent. Returning previous success.")
                return self.idempotency_map[idempotency_key]['result']

            wallet = self._get_or_create_wallet(user_id)
            
            time.sleep(random.uniform(0.01, 0.05)) 
            
            # Atomic Read-Modify-Write
            new_balance = wallet['balance'] + amount_coins
            wallet['balance'] = new_balance
            
            operation = {
                'type': 'reward',
                'amount': amount_coins,
                'reward_id': reward_id,
                'key': idempotency_key,
                'time': time.time()
            }
            wallet['operations'].append(operation)
            
            # Record success
            result_message = f"Reward successful. Added {amount_coins} coins. New balance: {new_balance}"
            self.idempotency_map[idempotency_key] = {
                'status': 'completed',
                'result': result_message,
                'timestamp': time.time()
            }
            print(f"[{user_id}] ✅ Reward processed. Amount: {amount_coins}. New Balance: {new_balance}")
            
            return result_message


    def get_wallet(self, user_id: str):
        """
        GET /wallet/:userId: Returns current balance and recent operations (last 5).
        """
        # Read-only operation still benefits from the lock to ensure we read a fully committed state.
        with self.lock:
            wallet = self.wallets.get(user_id)
            if not wallet:
                return {'userId': user_id, 'balance': 0, 'recent_operations': []}
            
            # Return last 5 operations
            recent_ops = sorted(wallet['operations'], key=lambda x: x['time'], reverse=True)[:5]
            
            return {
                'userId': user_id,
                'balance': wallet['balance'],
                'recent_operations': recent_ops
            }

# --- Concurrency Test & Run Instructions ---

def run_concurrency_test(service: WalletService, test_user_id: str):
    print("\n--- Starting Concurrency Test ---")
    
    initial_deposit = 100
    topup_amount = 25.00
    reward_amount = 50
    expected_balance = initial_deposit + int(topup_amount) + reward_amount
    
    # Initialize wallet balance
    service.topup(test_user_id, initial_deposit, "initial-setup-key")
    initial_state = service.get_wallet(test_user_id)['balance']

    # --- Concurrent Calls Setup ---
    
    t1 = threading.Thread(
        target=service.topup, 
        args=(test_user_id, topup_amount, "topup-key-1")
    )
    
    t2 = threading.Thread(
        target=service.game_reward, 
        args=(test_user_id, reward_amount, "snake-001", "reward-key-2")
    )

    print(f"Initial Balance: {initial_state} Coins")
    print(f"Expected Final Balance: {expected_balance} Coins ({initial_state} + {int(topup_amount)} + {reward_amount})")
    print("Firing Topup (T1) and Reward (T2) concurrently...")
    
    # Start both threads
    t1.start()
    t2.start()
    
    # Wait for both threads to complete
    t1.join()
    t2.join()
    
    # --- Verification ---
    final_state = service.get_wallet(test_user_id)
    final_balance = final_state['balance']
    
    print("\n--- Verification ---")
    print(f"Final Balance: {final_balance} Coins")

    if final_balance == expected_balance:
        print("✅ Success: Final balance is correct. Atomic updates worked.")
    else:
        print(f"❌ Failure: Final balance ({final_balance}) does not match expected balance ({expected_balance}).")
        
    print(f"\nRecent Operations: {final_state['recent_operations']}")
    
    # --- Idempotency Test ---
    print("\n--- Starting Idempotency Test (T2 retry) ---")
    
    # T2 Retries with the SAME key
    retry_result = service.game_reward(test_user_id, reward_amount, "snake-001", "reward-key-2")
    final_balance_after_retry = service.get_wallet(test_user_id)['balance']
    
    print(f"Retry Result: {retry_result}")
    print(f"Final Balance after Retry: {final_balance_after_retry} Coins")

    if final_balance_after_retry == expected_balance:
        print("✅ Success: Balance did not change. Idempotency worked.")
    else:
        print("❌ Failure: Balance was incorrectly credited again.")


if __name__ == "__main__":
    service = WalletService()
    TEST_USER_ID = "user-abc-123"
    
    run_concurrency_test(service, TEST_USER_ID)

    print("\n" + "="*50)
    print("--- Submission Explanation ---")

# --- Required Explanation Paragraph (The third deliverable) ---

explanation_paragraph = """
The primary race condition avoided is the **Lost Update** problem, a classic Read-Modify-Write concurrency issue. If two concurrent threads (e.g., Topup and Reward) read the user's balance simultaneously, modify it independently, and then both write their result back, the change from one thread will be overwritten and lost. My code prevents this by using a `threading.Lock`. By wrapping the critical section—which includes checking the idempotency map, reading the current balance, performing the arithmetic calculation, and writing the new balance—inside a `with self.lock:` block, only one thread can execute this section at a time, guaranteeing the atomic nature of the update. The idempotency approach uses an in-memory `idempotency_map` to store the success state and result of every processed request, keyed by the unique `idempotencyKey`. The map is checked *before* any balance modification. If the key exists, the original successful result is immediately returned without performing the credit again. This prevents duplicate credits, even if a client retries the request due to a perceived network failure.
"""

print(explanation_paragraph)
print("="*50)

print("\n--- Run Instructions ---")
print("1. Save the code above as `wallet_service.py`.")
print("2. Run the script from your terminal using: `python3 wallet_service.py`")
print("The script will initialize the service, run the concurrent test, verify the final balance, and test for idempotency.")