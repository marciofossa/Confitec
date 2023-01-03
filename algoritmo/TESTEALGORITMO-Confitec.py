#!/usr/bin/env python
# coding: utf-8

# In[1]:


import random


# In[5]:


matriz_A = []
matriz_B = []
matriz_produto = []
for i in range(4):
    linha1 = []
    linha2 = []
    for j in range(4):
        linha1.append(random.randint(0,100))
        linha2.append(random.randint(0,100))
    matriz_A.append(linha1)
    matriz_B.append(linha2)
print("Matriz A:",matriz_A)
print("Matriz B:",matriz_B)
# Produdo
for i in range(4):
    linha = []
    for j in range(4):
        linha.append(matriz_A[i][j]*matriz_B[i][j])
    matriz_produto.append(linha)
print("Matrix Produto:",matriz_produto)


