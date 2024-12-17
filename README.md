# Esercizio SDCC sul pattern MapReduce. 
In questa repository è salvata l'applicazione distribuita che implementa il paradigma MapReducer, in cui tramite chiamate RPC (Remote Procedure Call) implementiamo la comunicazione fra Client, Master e Workers. 
# Overview 
L'applicazione genera utilizza un dataset generato dal client per il sorting tramite il pattern MapReduce. Il datasert viene comunicato al Master che procede alla implementazione dei workers per la divisione dei compiti da svolgere. 
#### Client: 
Genera un dataset elementi random e lo invia al master. 
#### Master:
Divide l'array che riceve dal client in diversi chunk, nello specifico 5, che è pari al numero di workers che andiamo a instanziare. Invia i vari chunks ai worker che li attendono. 
#### Map Phase: 
Ricevuti i chunk i workers mappano la coppia chiave valore del rispettivo.
#### Reduce Phase:
I worker si adoperano per creare dei sottoinsiemi ordinati da rispedire al master. 
#### Risultato Finale: 
Il master scrive su un file result.txt i valori ordinati ricevuti da tutti i workers e li condivide con il client. 

# Architettura
- Client: genera un array con taglia fissa, con valori compresi fra 1 e 100.
- Master: divide in chuks il dataset ricevuto. Il numero dei chunks in cui viene diviso è basato sul numero di workers che andranno a operare la riduzione.
- Workers: si occupano di entrambe le fasi di Map e Reduce, in cui vanno a dividere e ordianre i valori che gli sono stati inviati dal master. Producono come output un sottoinsieme di valori che rispediscono al master. 


# Installazione ed Esecuzione dell'applicazione
##  Prerequisiti 
#### GO {versione 1.23.4 o successive} 

Per eseguire il codice lanciare da terminale: 
1. go run master.go
2. go run workers.go --ID=1 --Port=8000
3. go run workers.go --ID=2 --Port=8000
4. go run workers.go --ID=3 --Port=8000
5. go run workers.go --ID=4 --Port=8000
6. go run workers.go --ID=5 --Port=8000
7. go run client.go

Il file con la versione finale dell'output è possibile trovarlo all'interno del folder MapReduce. 

   
