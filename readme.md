# Birman-Schiper-Stephenson protocol

This is Birman-Schiper-Stephenson protocol study implementation on top of RabbitMQ. 

### How to run locally
Create Python 3 virtual environment:
```
python3 -m venv venv
```
Activate virtual environment:
```
source env/bin/activate
```
Install requirements:
```
pip install -r requirements.txt
```
Run several N - 1 clients by:
```
./add_client.bash
```
Run N-th client which will start session by:
```
./start.bash
```