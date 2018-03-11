EmailCounterMapper: The mapper class which reads each record, filters the Content-type of response and looks for text responses.
Then on the body applies the regex pattern and looks at the url and creates the text output as a pair of email and url to the reducer.

Configured number of reduce count to 10.

WARCEmailCounter: The main class which defines the job configurations,

MyReducer: Gets a collection of iterables of url for every email, gets the unique url count from the set and writes it's count to the context.

Library to read warc files: 
https://github.com/vadali/warc-mapreduce/tree/master/java/edu/cmu/lemurproject

IDE used: IntelliJ and Maven for dependency management and creating jars.
Java Version :JDK 8
Hadoop Version : 2.7.x



Note: 
1)Since I use maven for creating the jar file, without the entire project repo, it will be hard to construct the Jar. If needed I can also submitting a zip of the repo, to execute on EMR if needed.
2)Also include pom.xml for your reference


Extract the project zip, and run 
¬	mvn clean
¬	mvn install
The jar file is available in sub directory target/


