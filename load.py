import sys

from node_normalizer.loader import load_all


if __name__ == "__main__":
    success = load_all(100_000)
    if not success:
        print("Failed to load node normalization data.")
    else:
        print("Success")
    sys.exit(0 if success else 1)
