# BatchRunner
A small jar that helps running a lot of java jobs in parallel.
## Usage
`java -jar batchrunner.jar -jarFile <jar to run> -configFolder <folder with config files> -fileType <the ending of config files, e.g. .xml>`
runs a given jar once for every config file found in the config folder that matches the given file type 

`java -jar batchrunner.jar -help` prints help and all possible parameters
