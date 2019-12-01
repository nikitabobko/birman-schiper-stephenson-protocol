# Birman-Schiper-Stephenson protocol

This is Birman-Schiper-Stephenson protocol study implementation on top of RabbitMQ. 
Protocol provides causality in Distributed Systems. This practice was performed as 
part of "Distributed Systems" course in CMC MSU.

### How to run locally
Create Python 3 virtual environment, and install requirements:
```
./setup.bash
```
Activate environment:
```
source env/bin/activate
```
Run first N - 1 clients by:
```
./add_client.bash
```
Run N-th client which will start session:
```
./start.bash
```
