import sys

from node_normalizer.loader import load_all

# Number of records buffered per Redis pipeline before it is flushed. Larger
# batches mean fewer round trips (faster) at the cost of more memory per batch.
REDIS_PIPELINE_BATCH_SIZE = 100_000


if __name__ == "__main__":
    success = load_all(block_size=REDIS_PIPELINE_BATCH_SIZE)
    if not success:
        print("Failed to load node normalization data.")
    else:
        print("Success")
    sys.exit(0 if success else 1)
