# Cross-Game-Wallet-Assignment

This repository contains a minimal, in-memory backend implementation for a cross-game wallet, satisfying the requirements for atomic updates, idempotency, and minimal code footprint.

The solution is implemented in Python and includes the server logic and the required concurrency test client within a single file.

# 1. Source Code (wallet_service.py)

The core logic, API endpoints, and concurrency test are contained in this single file:
[ Watch code that is mentioned in wallet_service.py ]

# 2. Explanation of Constraints

The Race Condition and Prevention

The primary race condition avoided is the Lost Update problem, a classic Read-Modify-Write concurrency issue. If two concurrent threads (e.g., Topup and Reward) read the user's balance simultaneously, modify it independently, and then both write their result back, the change from one thread will be overwritten and lost. My code prevents this by using a threading.Lock. By wrapping the critical section—which includes checking the idempotency map, reading the current balance, performing the arithmetic calculation, and writing the new balance—inside a with self.lock: block, only one thread can execute this section at a time, guaranteeing the atomic nature of the update. The idempotency approach uses an in-memory idempotency_map to store the success state and result of every processed request, keyed by the unique idempotencyKey. The map is checked before any balance modification. If the key exists, the original successful result is immediately returned without performing the credit again. This prevents duplicate credits, even if a client retries the request due to a perceived network failure.


# 3. Run Instructions

To run the server and the simple concurrency test:

Ensure you have Python  installed.

Save the code as wallet_service.py (or ensure you are in the directory containing the file).

Execute the file from your terminal:

python wallet_service.py


The output will display the test results, confirming the atomic updates and idempotency checks were successful.
