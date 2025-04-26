# Blobby Tombstone Implementation Plan (TDD Approach)

## Initial Changes and Tests for Data Structures

1. Write unit tests for the modified `Record` struct in `pkg/types/types_test.go` to verify that tombstone records should serialize and deserialize correctly. These tests will initially fail. Include tests to verify the `Tombstone` field defaults to `false` for backward compatibility.

2. Modify the `Record` struct in `pkg/types/types.go` to add the `Tombstone` boolean field (defaulting to `false`), and make the `Document` field optional with the `omitempty` tag.

3. Update the `Read` and `Write` methods in `pkg/types/types.go` to handle the new tombstone field during serialization/deserialization, making the tests from step 1 pass. Ensure tombstone records have their `Document` field explicitly set to `nil` or `[]byte{}`, not just marked with `omitempty`.

## Memtable Interface and Testing Updates

4. Add unit tests for tombstone support in `pkg/memtable/memtable_test.go`. These tests will initially fail. Include tests for creating tombstone records with nil document data.

5. Extend the `Put` method signature in the `Memtable` interface in `pkg/memtable/memtable.go` to accept a `tombstone` parameter. Add a temporary stub implementation that panics when `tombstone` is true, to ensure the code still compiles. Identify and update all existing callers of the `Put` method to pass `tombstone: false` as the new parameter.

6. Update the `Memtable.Put` implementation to handle the tombstone parameter and create records with the appropriate flag set, making the tests from step 4 pass. Ensure that when `tombstone` is true, the `Document` field is set to `nil`.

## API Extensions and Tests

7. Add the `Delete` method to the `Blobby` interface in `pkg/api/blobby.go` with the specified signature: `Delete(ctx context.Context, key string) (string, error)`.

8. Update the `FakeBlobby` in `pkg/blobby/testutil/fake_blobby.go` to implement the new `Delete` method for testing purposes. Ensure it correctly adds tombstone records to its internal data structure.

9. Add unit tests for the `Delete` method in `pkg/blobby/archive_test.go`. These tests will initially fail. Include tests for deleting existing keys, deleting non-existent keys, and deleting already-deleted keys.

10. Implement the `Delete` method in `pkg/blobby/archive.go` to call the modified `Memtable.Put` with the tombstone flag set to true and empty document, making the tests from step 9 pass.

## Read Path Tests and Modifications

11. Add unit tests for the modified record reading behavior in `pkg/blobby/archive_test.go` to verify that tombstones should mask previous versions of keys. These tests will initially fail. Include tests for:
    - Getting a deleted key should return nil
    - Getting a key that was deleted then re-added should return the new value
    - Multiple tombstones for the same key (only the newest should matter)

12. Modify the `Get` method in `pkg/blobby/archive.go` to check for tombstone records and return nil when a tombstone is encountered. Ensure it correctly handles cases where there are multiple tombstones for the same key (by keeping the newest one).

13. Update the `Scan` method in `pkg/blobby/archive.go` to handle tombstone records appropriately during record scanning, making the tests from step 11 pass.

## SSTable Writer Tests and Updates

14. Add unit tests for the SSTable writer's handling of tombstone records in `pkg/sstable/writer_test.go`. These tests will initially fail. Include tests to verify that tombstones are ordered correctly (tombstones should appear before any other records with the same key but older timestamps).

15. Extend the `Writer.Add` method in `pkg/sstable/writer.go` to handle tombstone records in the written SSTable.

16. Update the sorting logic in `Writer.Write` to ensure tombstone records are ordered correctly in relation to other records with the same key, making the tests from step 14 pass. Pay special attention to key/timestamp ordering to guarantee that tombstones mask older records with the same key.

## SSTable Reader Tests and Updates

17. Add tests for the SSTable reader's handling of tombstone records in `pkg/sstable/reader_test.go`. These tests will initially fail.

18. Update the `Reader` in `pkg/sstable/reader.go` to properly read tombstone records, making the tests from step 17 pass.

## Filter Updates for Tombstones

19. Add tests for bloom filter behavior with tombstones in `pkg/filter/xor/xor_test.go` and/or `pkg/filter/mod/mod_test.go`. Verify that filters properly index tombstone records so the system can determine if a key might have been deleted.

20. Update the filter implementations to ensure they index tombstone records correctly, making the tests from step 19 pass.

## Compaction Tests and Updates

21. Add tests for the compaction behavior with tombstones in `pkg/compactor/compactor_test.go`. These tests will initially fail. Include tests to verify that:
    - Tombstones are preserved in the output only if they are the most recent version of a key
    - If there's a newer non-tombstone record with the same key, the tombstone should be discarded
    - Older versions of keys with tombstones are dropped during compaction

22. Modify the `Compact` method in `pkg/compactor/compactor.go` to track keys with tombstones during the compaction process.

23. Update the compaction logic to preserve tombstone records while dropping older versions of keys that have tombstones, making the tests from step 21 pass.

## Integration Testing

24. Add integration tests in `pkg/blobby/archive_test.go` that verify the entire delete workflow:
    - Basic delete functionality
    - Delete followed by get behavior
    - Delete of non-existent keys
    - Delete followed by put with new value (verify the key can be re-added after deletion)
    - Performance with many tombstones

25. Update the chaos testing in `pkg/blobby/archive_chaos_test.go` to include delete operations. These tests will initially fail. Add a test case for deleting a key that doesn't exist to verify it creates a tombstone that prevents the key from being added in the past.

26. Fix any remaining issues to make all integration tests pass.

## CLI and Tool Updates

27. Add unit tests for the delete command in `cmd/archive/main_test.go`. These tests will initially fail.

28. Add the delete command to the CLI interface in `cmd/archive/main.go`.

29. Implement the `cmdDelete` function in `cmd/archive/main.go` to handle delete operations from the command line, making the tests from step 27 pass.

## Future Compatibility Considerations

30. Document considerations for future range query support with tombstones. This should address:
    - How range queries would handle tombstones
    - Whether tombstones should be included in range query results
    - How to maintain backward compatibility with existing code

## Final Testing and Documentation

31. Run all existing tests to ensure backward compatibility is maintained.

32. Add documentation for the delete functionality in the README.md file.

33. Create examples demonstrating the delete functionality in the README.md file.
