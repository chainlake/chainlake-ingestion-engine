    # bounded streaming loop
    async def run_batch(self, start, end):
        await self.sink.start()

        in_flight = set()
        next_block = start

        while next_block <= end or in_flight:
            # fill up concurrency window
            while len(in_flight) < self.semaphore._value and next_block <= end:
                task = asyncio.create_task(self._run_one(next_block))
                in_flight.add(task)
                next_block += 1

            # wait for at least ONE task to finish
            done, in_flight = await asyncio.wait(
                in_flight,
                return_when=asyncio.FIRST_COMPLETED
            )

        await self.sink.close()
        self._print_summary()
        
        

    def _print_summary(self):
        if not self.metrics:
            return

        summary = self.metrics.summary()

        print("\n==============================")
        print(" GLOBAL METRICS")
        print("==============================")
        print(f"Elapsed (s)       : {summary['elapsed_sec']:.2f}")
        print(f"Total requests    : {summary['requests']}")
        print(f"Success           : {summary['success']}")
        print(f"Errors            : {summary['errors']}")
        print(f"RPS               : {summary['rps']:.2f}")
        print(f"Avg latency (ms)  : {summary['avg_latency']:.2f}")
        print(f"P95 latency (ms)  : {summary['p95_latency']:.2f}")
        print(f"Avg payload (KB)  : {summary['avg_payload_kb']:.2f}")
        print(f"Avg tx count      : {summary['avg_tx']:.2f}")