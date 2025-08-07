from typing import Tuple


def longest_unique_substring(s: str) -> Tuple[str, int]:
    start = 0
    max_len = 0
    max_start = 0
    seen = {}

    for end, char in enumerate(s):
        last_seen = seen.get(char)
        if last_seen is not None and last_seen >= start:
            start = last_seen + 1
        seen[char] = end

        current_len = end - start + 1
        if current_len > max_len:
            max_len = current_len
            max_start = start

    return s[max_start : max_start + max_len], max_len


def main() -> None:
    s = input("Input: ").strip()
    substring, length = longest_unique_substring(s)
    print(f"Output: {substring} Length: {length}")


if __name__ == "__main__":
    main()
