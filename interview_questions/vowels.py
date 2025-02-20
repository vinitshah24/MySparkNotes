def move_vowels(arr):
    """Moves vowels in the beginning"""
    vowels = ["a", "e", "i", "o", "u"]
    l = 0
    r = len(arr) - 1
    while l < r:
        if arr[l] in vowels:
            l += 1
        elif arr[r] not in vowels:
            r -= 1
        else:
            arr[l], arr[r] = arr[r], arr[l]
            l += 1
            r -= 1
    return arr


def test_move_vowels():
    arr = ["a", "x", "r", "e", "p", "i", "o", "t", "u"]
    res = move_vowels(arr)
    assert res == ["a", "u", "o", "e", "i", "p", "r", "t", "x"]


data = []
with open("data/fakefriends.csv") as f:
    for line in f:
        data.append(line.strip().split(","))


for line in data[:2]:
    print(line)
