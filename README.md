## **Universidade Federal do Ceará** | **Departamento de Computação**
### **Disciplina: Engenharia de Sistemas Inteligentes (CK0444 – 2025.1)** 
### **Professor: Lincoln Souza Rocha | E-mail: lincoln@dc.ufc.br**

**Prática ELT de Dados Sistema de Predição de Defeitos**

1. Crie um token de acesso no GitHub. Para isso, vá até "Settings" >> "Developer settings" >> "Personal access tokens" >> "Fine-grained personal access tokens" >> "Generate new token".

2. Copie o token gerado e cole onde indicado na linha de código ``263`` de ``sdp_elt_data_pipeline.py``. 

3. Instale as dependências necessárias para rodar a pipeline ELT de dados:
```
$ pip install -r requirements.txt
```

4. Execute a pipeline ELT de dados:
```
$ python sdp_elt_data_pipeline.py
```

5. Agora, implemente a função ``tansform_raw_dataset(...)`` de ``sdp_elt_data_pipeline.py``.

6. Execute novamente a pipeline ELT de dados:
```
$ python sdp_elt_data_pipeline.py
```