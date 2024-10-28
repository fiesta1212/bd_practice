import sys
import re
from collections import Counter

def count_words(file_path):
    with open(file_path, 'r', encoding='cp1251') as file:
        text = file.read()

    words = re.findall(r'\b\w+\b', text)
    word_counts = Counter(words)

    filtered_counts = {word: count for word, count in word_counts.items() if len(word) >= 4 and count >= 10}

    sorted_counts = sorted(filtered_counts.items(), key=lambda item: (-item[1], item[0]))

    result = [f"{word.lower()} - {count}" for word, count in sorted_counts]
    return result

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)

    file_path = sys.argv[1]
    results = count_words(file_path)

    for line in results:
        print(line)