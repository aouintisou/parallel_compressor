# Parallel File Compressor

**Module** : Traitement Intensif, Parallèle & Benchmarking  
**Binôme** : Soulaima / ____________  
**École** : ESPRIT 2024/2025

## Description
Outil de compression/décompression parallèle de fichiers en C
utilisant les API POSIX : fork, pthreads, sémaphores, pipes, 
message queue et mémoire partagée.

## Concepts implémentés
- Multiprocessus configurables : fork()
- Multithreads configurables : pthread_create()
- IPC Queue : msgget / msgsnd / msgrcv
- IPC Pipe : pipe() / read() / write()
- Mémoire partagée : shmget / shmat
- Sémaphore POSIX : sem_wait / sem_post
- Problème Barbier Endormi : dispatcher + sem_wait
- Benchmarking : 7 configurations comparées

## Installation
sudo apt install zlib1g-dev
make

## Utilisation
./compressor --processes 4 --threads 2 --mode compress
./compressor --processes 4 --threads 2 --mode decompress --input output_files --output decompressed
./compressor --benchmark --mode compress
