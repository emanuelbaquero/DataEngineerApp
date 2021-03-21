FROM ubuntu:latest

RUN apt-get update 
RUN apt install -y python3-pip
RUN pip3 install flask 
RUN pip3 install flask-restful 
RUN pip3 install pandas 
RUN pip3 install dnspython 
RUN pip3 install requests 
RUN pip3 install pymongo 
RUN pip3 install flask_restplus 
RUN pip3 install Werkzeug==0.16.1 

WORKDIR /work

COPY bar.sh /work
COPY generar_archivo.py /work
COPY guardar_datos_mongo.py /work
COPY app/rest.py /work 
COPY start.sh /work 
COPY st_regiones.csv /work

RUN ["bash", "bar.sh"]
RUN ["python3","generar_archivo.py"]

CMD ["bash", "start.sh"]



