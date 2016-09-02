# days
# datascience
Scalable Document Classification
--------------------------------

Team Name: DAYS

Team Members:
- Yash Shrivastava
- Dharamendra Prajapati
- Sharmin Pathan

Technologies Used:
- Spark Core
- Spark SQL
- Resilient Distributed Datasets (RDDs)
- DataSets
- Java 1.8

Preprocessing:
The training subsets are filtered for removal of
- stopwords: A properties file that contains a list of stopwords that don't contribute to the classification process like articles, prepostions, etc
- special characters

Vocabulary List:
After filtering the training subsets, a vocabulary has been built for the classification process.

Classification Process:
Naive Bayes Classifier has been applied.
Probabilities of every word appearing in the vocabulary list against every document and the category label assigned for that document is calculated. The documents and category label probabilities have also been computed.
The classifier on being applied to the testing subsets, gives its classification category.

Smoothing:
Applied to the testing subsets to avoid the worst case of 0 probability for a word that appears in the testing subsets but was not in the training subset.

Flow:
The programe works in two different modes (LEARNING and TESTING) that are specified during the execution of the program though command line arguments.
LEARNING:-takes 4 command line arguments:-
args[0]- path of the trainer file
args[1]- path of the classifer file (Classifed lables)
args[2]- path to store the vocab probability file
args[3]- path to store the lable probability file
args[4]- LEARNING 

TESTING:-takes 4 command line arguments:- 
args[0]- path of the test file
args[1]- path of vocab probablity file that was generated using the LEARNING mode
args[2]- path of lable probablity file that was generated using the LEARNING mode
args[3]- path to store the final file that contains the classification for every document
args[4]- TESTING

Instructions to install and run the programe:-
To run the program, download the .jar file from Github and execute it using Java8 providing the correct command line arguments.
 


