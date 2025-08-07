# Longest Substring Without Repeating Characters
This is a simple Python application that finds the length of the longest substring without repeating characters in a given input string.

## Problem Description
Given a string, find the length of the longest substring without repeating characters. For example:

Input:  abcabcbb  
Output: abc  Length: 3

## How to Run

1. Firstly, navigate to the project directory:

```bash
cd q3-algo-longest-substring-no-dup
```

2. Run the script:

```bash
python solution.py
```

Enter your input string when prompted. The script will output the length of the longest substring without repeating characters.


## Time Complexity: O(n)
    # - iterated through the string once using a sliding window.
    # - The start pointer only ever moves forward.
    # - Each character is processed at most twice (once when seen, once when skipped), resulting in linear time.

## Space Complexity: O(min(n, m))
    # - used a dictionary seen to store the last seen index of characters.
    # - In the worst case (all characters unique), we store up to n characters.
    # - If the character set is fixed (e.g., ASCII(basic eng letters,digits etc)), space is bounded by m, the charset size (constant for ASCII, 128 or 256).
    # - So, space complexity is O(min(n, m)).


## Project Structure
```bash
q3-algo-longest-substring-no-dup/
│
├── solution.py      # Main script to run the algorithm
├── description.md   # Detailed problem explanation and examples
└── README.md        # this documentation
```