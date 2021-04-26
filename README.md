## Repo Content
* [Performance test for loading ~700M KV´s records into a RockDB](#Repo-Scope)
* [RockDB approaches to load data](#approaches)
* [Next steps](#whatnext)
* [Technology & Setup](#techsetup)

## Performance test for loading ~700M KV´s records into a RockDB

·         So it took ~3h to load

·         Split the csv file into 12 files. Each file having around 60M rows/entries

·         Split the process into 4 threads, each thread having 3 of that 12 files as its workload

·         After compacting, the RockDB occupies 58 GB on disk

## RockDB approaches to load data

·         Writing in Batch mode: very fast for ingesting but tricky to fine tune the memory usage - bad parameters potentially cause OOM crashes

·         Writing in entry-by-entry: this doubles the loading time compared to batch mode but not so limited by memory, even running multiple writing tasks in parallel

*Both approaches are able to perform the tasks in multiple threads in one single process

## Next steps

·         stressing the retrieving operations in parallel to the writing ones

·         fine tunning the mem/caching parameters, parallelism limitations

## Technology & Setup

·         RockDB

·         Java
