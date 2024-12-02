# useful_snippets
Useful code snippets

## Python
### large_parquet_loader
The standard Python libraries are slow at reading large parquet files. Some methods scan the entire dataset when reading in the data which is computationally expensive and can also cause crashes if the file is larger than memory. A class along with examples are given that load the data by utilising the Hive partitioning structure, speeding up the read process and reducing the risk of memory errors.

### force_reload_module_in_notebook
Using the extention `autoreload` often doesn't work as expected or doesn't reload nested imports, which can cause issues when developing the code. This snippet gives and example of how to force the package to be reloaded. This can also be done for any nested imports.

## SQL