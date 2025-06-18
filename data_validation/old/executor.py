from concurrent.futures import wait, FIRST_COMPLETED
import logging



def wait_and_fill_futures(executor, futures, batch_gen, summary, max_futures):
    while futures:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)

        for future in done:
            try:
                result = future.result()
                summary["total_count"] += result["count"]
                summary["missing_in_mongo"] += result["missing_in_mongo"]
                summary["missing_in_arango"] += result["missing_in_arango"]
                summary["field_mismatches"] += result["field_mismatches"]
            except Exception as e:
                logging.exception("Batch execution failed", exc_info=e)

            # Immediately replace finished future with a new batch
            try:
                next_batch = next(batch_gen)
                new_future = executor.submit(process_batch, next_batch)
                not_done.add(new_future)
            except StopIteration:
                pass  # No more batches to submit

        futures = list(not_done)

batch_gen = batch_generator(arango_cursor, BATCH_SIZE)
futures = []

# Initially fill the queue with NUM_THREADS + 2
for _ in range(NUM_THREADS + 2):
    try:
        batch = next(batch_gen)
        futures.append(executor.submit(process_batch, batch))
    except StopIteration:
        break

wait_and_fill_futures(executor, futures, batch_gen, summary, NUM_THREADS + 2)

