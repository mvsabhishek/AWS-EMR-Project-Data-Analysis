# Managing-the-cloud-IFT-598-Project
Managing the Cloud - with Amazon Web Services

Please refer the Word Document in the repository for detailed description.

Summary:

The objective of the project was to analyze historic crime data of the city of Los Angeles to understand 
the patterns of crimes using cloud services. The project was divided various stages. 

Firstly, the dataset of historic crime data of the city of Los Angeles was downloaded from data.gov website. 

Secondly, the dataset was stored on Amazon S3. 

Thirdly, Mapper, Reducer and Driver classes were written in Java to analyze the data in the dataset. 

Fourthly, Amazon EMR, a distributed data processing system in cloud, was setup. 
The EMR was configured with the location of output and the dataset on Amazon S3. The location of the JAR 
file on Amazon S3 containing all the Mapper, Reducer and Driver classes was added to the EMR during configuration. 
The EMR was setup in step execution mode so that creating a new instance of EC2 for each use case could be avoided.

Lastly, the output from the EMR was visualized using Tableau. To do so, a database was created on top of the
Amazon S3 bucket using Amazon Athena. Then, Tableau desktop was connected directly to the data source stored as 
data tables on Amazon Athena. The data was used to create dashboards that comprehensively displayed the results
of the analysis. 

To achieve the objective, various cloud technologies such as Amazon S3, Amazon EMR, and Amazon Athena, and 
essential technologies for data analysis such as Map Reduce using Java and Apache Hadoop were learnt.
