**Video sustentacion:**  [Video](https://eafit-my.sharepoint.com/:v:/g/personal/dahernando_eafit_edu_co/EZuCNupRugNLoYmeCgocOhYBCRarVjVR2Y2WyDhXmv9gcA?e=TFVRrJ&nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJTdHJlYW1XZWJBcHAiLCJyZWZlcnJhbFZpZXciOiJTaGFyZURpYWxvZy1MaW5rIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXcifX0%3D)

 

# Creacion del EMR

Para este laboratorio es necesario crear un EMR desde AWS CLI, para esto, se deben seguir el siguiente conjunto de pasos:

**Crear una clave par:** Esto se hace con el objetivo de poder acceder a la instancia a traves de ssh de forma segura, para lobra esto se debe ejecutar el siguiente comando ```aws ec2 create-key-pair --key-name <MyKeyPairName> --key-type rsa --key-format pem --query "KeyMaterial" --output text > <MyKeyPairName>.pem```  Hay que tener en cuenta que ```<MyKeyPairName>``` es el nombre que se le asigna a la llave


**Crear bucket s3:** Esto se hace con el objetivo de guardar los logs del EMR, para realizar esta creacion, ejecuatmos el siguiente comando ```aws s3api create-bucket --bucket <bucketName> --region us-east-1``` debido a que la cuenta es de estudiante solo se puede usar la region ```us-east-1``` ademas ```<bucketName>``` es el nombre que le damos al bucket 

**Creacion EMR:** El ultimo paso es crear el cluster EMR donde vamos a ejecutar los codigos, para esto, se ejecuta el siguiente comando 
```
aws emr create-cluster --release-label "emr-7.1.0" --name <EMRName>--applications Name=Spark Name=Hadoop Name=Pig Name=Hive --ec2-attributes KeyName=<MyKeyPairName> --instance-type m5.xlarge --instance-count 3 --use-default-roles --no-auto-terminate --log-uri "s3://<bucketName>"
``` 
debemos tener en cuenta las variables que se mencionaron en los puntos anteriores, ademas, ```<EMRNAME>``` es el nombre que le deseamos colocar al EMR

**Ejecutar codigos en el cluster**

Se debe seleccionar el programa a ejecuatr y pasarle los datasets, corriendo el siguiente comando
``` 
python wordcount-mr.py hdfs:///user/hadoop/data/gutenberg-small/* -r hadoop --output-dir hdfs:///user/hadoop/output/results3.txt -D mapred.reduce.tasks=7
``` 