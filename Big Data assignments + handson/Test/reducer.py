#!/usr/bin/env python3

import sys

word_count = {}
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    if len(word) > 5:
        word_count[word] = word_count.get(word, 0) + int(count)

for word, count in word_count.items():
    print(f"{word}\t{count}")
