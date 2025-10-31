# import numpy as np
import random
import numpy as np

# Parameter settings
n = 160  # Number of dictionary entries
s = 0.2 * n  # Allowable deviation ratio
num_simulations = 100000  # Number of simulations

# Initialize statistics
position_scores = []

for _ in range(num_simulations):
    # Randomly generate target index and actual index
    t = random.randint(0, n-1)
    keys = list(range(n))
    random.shuffle(keys)
    a = keys.index(t)
    
    # Calculate position score
    position_diff = abs(a - t)
    position_score = 1 / (1 + (position_diff / s) ** 2)
    position_scores.append(position_score)

# Calculate expected value
E_position_score = np.mean(position_scores)
print(f"E[position_score]: {E_position_score}")
