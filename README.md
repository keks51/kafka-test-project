1) Build app using mvn
2) mv 'kafka-app.jar' to ./volume/jars/

How to run using Minikube:
1) REPLACE %PROJECT_HOME% with path to project. For example '/Users/user_name/IdeaProjects/kafka-test-project' in file ./cluster/hadoop.yaml

2) replace %PROJECT_HOME% 
minikube start --driver=hyperkit \
   --mount --mount-string="%PROJECT_HOME%/volume/:%PROJECT_HOME%/volume/" \
   --cpus 4 \
   --memory 10000 \
   --disk-size 40000mb
3) kubectl apply -f cluster/volume.yaml
4) kubectl apply -f cluster/config.yaml
5) kubectl apply -f cluster/hadoop.yaml
6) kubectl get pods
   ![Alt text](/images/get_pods.png)
7) minikube service list
   ![Alt text](/images/minikube_service_list.png)
8) go to airflow. username: admin. pass: admin
   ![Alt text](/images/airflow_1.png)
9) kafka producer dag is running and sending messages to orders topic
10) check orders topic by:
    ![Alt text](/images/go_to_kafka_ui.png)
11) start 'kafka_app_spark' dag
    ![Alt text](/images/start_kafka_app_dag.png)
12) check logs
INFO  SparkRW$:44 - Loading records from topic: orders using spark with offset range (0 - 1793)
INFO  SparkRW$:58 - Saving data in path: '/raw/orders/year=2022/week=22/'
13) check hdfs
    ![Alt text](/images/hdfs.png)
14) spark ui
    ![Alt text](/images/spark_ui.png)