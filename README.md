# BatchRunner
A small jar that helps running a lot of java jobs in parallel.
## Usage
`java -jar batchrunner.jar -jarFile <jar to run> -configFolder <folder with config files> -fileType <the ending of config files, e.g. .xml>`
runs a given jar once for every config file found in the config folder that matches the given file type 

`java -jar batchrunner.jar -help` prints help and all possible parameters

##License
BatchRunner is released under GPLv3 (or later). If you make changes to BatchRunner or use the code, you must release these changes under GPLv3+ (best would be to fork this project).
Nevertheless, you may use BatchRunner to run *any* jar (the license of the jar ran is not important).
