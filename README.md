# 6.824 Lab 1: MapReduce from Distributed System MIT

<!---Esses são exemplos. Veja https://shields.io para outras pessoas ou para personalizar este conjunto de escudos. Você pode querer incluir dependências, status do projeto e informações de licença aqui--->

![GitHub repo size](https://img.shields.io/github/repo-size/HuaTrung/Distributed-System-6.824-MapReduce?style=for-the-badge)
![GitHub stars count](https://img.shields.io/github/stars/HuaTrung/Distributed-System-6.824-MapReduce?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/HuaTrung/Distributed-System-6.824-MapReduce?style=for-the-badge)
![Bitbucket open issues](https://img.shields.io/github/issues/HuaTrung/Distributed-System-6.824-MapReduce?style=for-the-badge)
![Watchers](https://img.shields.io/github/watchers/HuaTrung/Distributed-System-6.824-MapReduce?style=for-the-badge)

<img src="./imgs/mapreduce.jpg" alt="map reduce">



## 💻 Introduction

Hi folks, its my implementation for [Mapreduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). The code bases on [Lab 1 6.824](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html), but I modified a little bit.
## 🚀 Functionality
 Besides the basic requirements of 6.824, I added a couple of things. Here it is:
- [x] Task Dispatch ( Map & Reduce task )
- [x] Parallelism
- [x] Heartbeat
- [x] Failed and Straggler Detection
- [x] Re-execution ( failed and straggler workers)
- [ ] Skipping Bad Records
- [ ] Status Information
- [ ] Counters


## 🚀 Running 

Para instalar o <nome_do_projeto>, siga estas etapas:

For master:
Linux, macOS & Window:
```
go run mrmaster.go pg-.*txt
```

For multiple workers:
Windows:
```
go run mrworker.go
go run mrworker.go
```

## 📝 License

The source code for the site is licensed under the MIT license, which you can find in the MIT-LICENSE.txt file.


