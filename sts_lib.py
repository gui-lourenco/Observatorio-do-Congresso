# Versão 1.1.2
import sqlite3 as sql
import scipy.stats as st
from matplotlib import pyplot as pp
import numpy as np
import time

# CONST GLOBAIS DE TEMPO
data_atual = time.localtime()[0:3]
# Dicionario com as variáveis globais
global_keys = dict()
global_keys['DATA_INICIO'] = '2010-02-24'
global_keys['DATA_FIM'] = '{}-{}-{}'.format(data_atual[0], data_atual[1], data_atual[2])
global_keys['SQL_VOTACAO_SORT'] = '''
	SELECT id_votacao
	FROM votacao
	WHERE date(dataHoraInicio) >= '{}'
	AND date(dataHoraInicio) <= '{}';
'''
global_keys['SQL_PARLAMENTAR_SORT'] = '''
	SELECT id_parlamentar
	FROM parlamentar
'''

# Funções auxiliares
def converte_data(data_string):
	""" Converte as data_strings em milisegundos a partir da epoch"""
	data = time.strptime(data_string , '%Y-%m-%d')
	data = time.mktime(data)
	return data

# Funções de Caulculos estatísticos
# Funções Relacionadas às votações
def assertividade_votacao(id_votacao):
	""" Calcula a assertividade de uma votação 
	 ex:
	 	sim = 20, não = 31, abs = 30
	 	return (não/sim+não+abs)*100"""

	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	sql_command = '''
		SELECT id_votacao
		FROM votacao_secreta 
		WHERE id_votacao = {};
	'''
	votacao = cursor.execute(sql_command.format(id_votacao)).fetchone()
	if votacao == None:
		sql_command = '''
			SELECT count(*) AS qtd
			FROM voto
			WHERE id_votacao = {} 
			GROUP BY descricao;
		'''
		votacao = cursor.execute(sql_command.format(id_votacao)).fetchall()
		votacao = [voto[0] for voto in votacao]
		if votacao == []:
			return None

		placar_max = max(votacao)
		total = sum(votacao)
		conn.close()
		return (placar_max/total)*100

	else:
		sql_command1 = '''
			SELECT placarSim, placarNao, placarAbs
			FROM votacao_secreta
			WHERE id_votacao = {};
		'''
		sql_command2 = '''
			SELECT count(*)
			FROM voto 
			WHERE id_votacao = {}
		'''
		v_secreta = cursor.execute(sql_command1.format(id_votacao)).fetchone()
		v_secreta = [x for x in v_secreta if x != None]
		if v_secreta == []:
			return None

		placar_max = max(v_secreta)
		total = cursor.execute(sql_command2.format(id_votacao)).fetchone()[0]
		conn.close()
		return (placar_max/total)*100

def total_votos(id_votacao, tipo):
	""" Dado uma votacção e uma descrição (sim, não, abstenção) retorna o número de votos
	da descrição na votação em questão"""
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	if tipo.lower() == 's': tipo = 'Sim'
	elif tipo.lower() == 'n': tipo = 'Não'
	elif tipo.lower() ==  'a': tipo = 'Abstenção'
	elif tipo.lower() not in ('Sim', 'Não', 'Abstenção'):
		raise ValueError("Tipo Inválido")

	sql_command = '''
		SELECT id_votacao
		FROM votacao_secreta
		WHERE id_votacao == {}
	'''
	votacao = cursor.execute(sql_command.format(id_votacao)).fetchone()
	if votacao == None:
		sql_command ='''
			SELECT count(*)
			FROM voto
			WHERE
				id_votacao = {} AND
				descricao = '{}';
			'''
		total = cursor.execute(sql_command.format(id_votacao,tipo)).fetchone()
		if total == None:
			total = 0

		return total[0]

	else:
		if tipo == 'Sim': tipo = 'placarSim'
		if tipo == 'Não': tipo = 'placarNao'
		if tipo == 'Abstenção': tipo = 'placarAbs'
		sql_command = '''
			SELECT {}
			FROM votacao_secreta
			WHERE id_votacao == {};
		'''
		total = cursor.execute(sql_command.format(tipo, id_votacao)).fetchone()
		conn.close()
		return total[0]

def competitividade_votacao(id_votacao, op = '/'):
	""" Dado uma votação e um operador('/','-') faz a razão ou a subtração da descrição mais votada pela segunda mais votada """
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	sql_command = '''
		SELECT id_votacao
		FROM votacao_secreta
		WHERE id_votacao == {}
	'''
	votacao = cursor.execute(sql_command.format(id_votacao)).fetchone()
	if votacao == None:
		sql_command = '''
			SELECT count(descricao) as qtd
			FROM voto
			WHERE id_votacao = {}
			GROUP BY descricao
			ORDER BY qtd desc
		'''
		primeiros = cursor.execute(sql_command.format(id_votacao)).fetchall()[:2]
		if primeiros == []:
			conn.close()
			return None
		
		if op == '/': calculo = int(primeiros[1][0])/int(primeiros[0][0])
		elif op == '-': calculo = int(primeiros[1][0])-int(primeiros[0][0])
		conn.close()
		return calculo
	
	else:
		sql_command = '''
			SELECT placarSim, placarNao, placarAbs
			FROM votacao_secreta
			WHERE id_votacao = {}
		'''
		primeiros = cursor.execute(sql_command.format(id_votacao)).fetchone()
		primeiros = [x for x in primeiros if x != None]
		if primeiros == []:
			conn.close()
			return None

		primeiros = sorted(primeiros)[-2:]
		if op == '/': calculo = int(primeiros[1])/int(primeiros[0])
		elif op == '-': calculo = int(primeiros[1])-int(primeiros[0])
		conn.close()	
		return calculo
	
def entropia(id_votacao):
	""" Dada uma votação calcula sua entropia"""
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	sql_command = '''
		SELECT id_votacao
		FROM votacao_secreta
		WHERE id_votacao == {}
	'''
	votacao = cursor.execute(sql_command.format(id_votacao)).fetchone()
	if votacao == None:
		sql_command = '''
			SELECT count(descricao) as qtd
			FROM voto
			WHERE id_votacao = {}
			GROUP BY descricao
		'''
		n_votos = cursor.execute(sql_command.format(id_votacao)).fetchall()
		n_votos = clean(n_votos, 0)
		n_votos = [int(a[0]) for a in n_votos]
		conn.close()	
		return st.entropy(n_votos)

	else:
		sql_command = '''
			SELECT placarSim, placarNao, placarAbs
			FROM votacao_secreta
			WHERE id_votacao = {}
		'''
		n_votos = cursor.execute(sql_command.format(id_votacao)).fetchone()
		n_votos = [n for n in n_votos if n != None]
		conn.close()	
		return st.entropy(n_votos)

def votacoes_periodo(passo = 'D', data_in = global_keys['DATA_INICIO'], data_fim = global_keys['DATA_FIM']):
	""" Retorna o número de votações numa faixa de tempo, podendo alternar o passo da contagem """
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	if passo == 'D':
		sql_command = '''
		SELECT date(dataHorainicio),count(id_votacao)
		FROM votacao
		WHERE date(dataHorainicio) >= '{}' AND
			date(dataHorainicio) <= '{}'
		GROUP BY date(dataHorainicio)
		ORDER BY date(dataHoraInicio);
	'''
		res = cursor.execute(sql_command.format(data_in,data_fim)).fetchall()
		res = [list(x) for x in res]
		conn.close()
		return res

	elif passo == 'M':
		sql_command = '''
			SELECT strftime('%Y-%m', dataHoraInicio) as tmp, count(*) as cnt
			FROM votacao 
			WHERE date(dataHorainicio) >= '{}' AND
				date(dataHorainicio) <= '{}'
			GROUP BY tmp
			ORDER BY date(dataHoraInicio);
		'''	
		res = cursor.execute(sql_command.format(data_in,data_fim)).fetchall()
		res = [list(x) for x in res]
		conn.close()
		return res

	elif passo == 'A':	
		sql_command = '''
			SELECT strftime('%Y', dataHoraInicio) as tmp, count(*) as cnt 
			FROM votacao 
			WHERE date(dataHorainicio) >= '{}' AND
				date(dataHorainicio) <= '{}'
			GROUP BY tmp
			ORDER BY date(dataHoraInicio);
		'''	
		res = cursor.execute(sql_command.format(data_in,data_fim)).fetchall()
		res = [list(x) for x in res]
		conn.close()
		return res

# Funções relacionadas às Matérias
def materias_periodo(passo = 'D', data_in = global_keys['DATA_INICIO'], data_fim = global_keys['DATA_FIM']):
	""" Retorna o número de votações numa faixa de tempo, podendo alternar o passo da contagem """
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	if passo == 'D':
		sql_command = '''
		SELECT date(data_apresentacao) as tmp, count(*)
		FROM materia
		WHERE date(data_apresentacao) >= '{}' AND
			date(data_apresentacao) <= '{}'
		GROUP BY tmp
		ORDER BY date(data_apresentacao);
	'''
		res = cursor.execute(sql_command.format(data_in,data_fim)).fetchall()
		res = [list(x) for x in res]
		conn.close()
		return res

	elif passo == 'M':
		sql_command = '''
			SELECT strftime('%Y-%m', data_apresentacao) as tmp, count(*) as cnt
			FROM materia 
			WHERE date(data_apresentacao) >= '{}' AND
				date(data_apresentacao) <= '{}'
			GROUP BY tmp
			ORDER BY date(dataHoraInicio);
		'''	
		res = cursor.execute(sql_command.format(data_in,data_fim)).fetchall()
		res = [list(x) for x in res]
		conn.close()
		return res

	elif passo == 'A':	
		sql_command = '''
			SELECT strftime('%Y', data_apresentacao) as tmp, count(*) as cnt 
			FROM materia 
			WHERE date(data_apresentacao) >= '{}' AND
				date(data_apresentacao) <= '{}'
			GROUP BY tmp
			ORDER BY date(dataHoraInicio);
		'''	
		res = cursor.execute(sql_command.format(data_in,data_fim)).fetchall()
		res = [list(x) for x in res]
		conn.close()
		return res

# Funções relacionadas aos parlamentares
def assertividade_parlamentar(id_parlamentar, data_in = global_keys['DATA_INICIO'], data_fim = global_keys['DATA_FIM']):
	""" Calcula a assertividade de um parlamentar em um período de tempo
	Assertividade é da pela quantidade de votos na descrição mais votada pelo parlamentar / total de votos do parlamentar"""
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	sql_command = '''
		SELECT count(*) as qtd 
		FROM parlamentar NATURAL JOIN voto
		WHERE id_votacao in 
			(SELECT id_votacao 
			FROM votacao 
			WHERE date(dataHoraInicio)>='{}' and date(dataHoraInicio)<='{}') 
			and id_parlamentar = {}
			and descricao != 'Votou'
		GROUP BY descricao;
	'''
	qtd = cursor.execute(sql_command.format(data_in, data_fim, id_parlamentar)).fetchall()
	qtd = [x[0] for x in qtd]
	total = sum(qtd)
	if total >= 40:
		maximo = max(qtd)
		assertividade = (maximo/total)*100
		conn.close()
		return assertividade

def numero_votos(id_parlamentar, data_in = global_keys['DATA_INICIO'], data_fim = global_keys['DATA_FIM']):
	""" Calcula o Número de Votos dado uma faixa de tempo """
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	sql_command = '''
		SELECT count(*) as qtd 
		FROM parlamentar NATURAL JOIN voto
		WHERE id_votacao in 
			(SELECT id_votacao 
			FROM votacao 
			WHERE date(dataHoraInicio)>='{}' and date(dataHoraInicio)<='{}')
		AND id_parlamentar = '{}'
		GROUP BY id_parlamentar;
		'''
	n_votos = cursor.execute(sql_command.format(data_in, data_fim, id_parlamentar)).fetchone()
	try:
		n_votos = n_votos[0]
	
	except TypeError:
		n_votos = 0

	conn.close()
	return n_votos

def chinelinho(id_parlamentar, data_in = global_keys['DATA_INICIO'], data_fim = global_keys['DATA_FIM']):
	""" Calcula uma porcentagem entre as licenças parlamentares e o total de votos """
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	sql_command = ''' 
		SELECT count(*) as qtd 
		FROM parlamentar NATURAL JOIN voto
		WHERE id_votacao in 
			(SELECT id_votacao 
			FROM votacao 
			WHERE date(dataHoraInicio)>='{}' and date(dataHoraInicio)<='{}')
			and id_parlamentar = {}
			and descricao like 'L%'
		'''
	sql_command2 = '''
		SELECT count(*) 
		FROM voto
		WHERE id_votacao in 
			(SELECT id_votacao 
			FROM votacao 
			WHERE date(dataHoraInicio)>='{}' and date(dataHoraInicio)<='{}')
			and id_parlamentar = {}
		'''
	licenca = cursor.execute(sql_command.format(data_in, data_fim, id_parlamentar)).fetchone()
	total = cursor.execute(sql_command2.format(data_in, data_fim, id_parlamentar)).fetchone()
	if total >= 40:
		chinelo = (licenca[0]/total[0])*100
		conn.close()
		return chinelo

# Função de Ordenação
def info_sort(func, sql_command, *args, **kwargs):
	"""Retorna uma lista ordenada tendo como parâmetro de ordenação uma das funções estatísticas
		args: data incial e data final usados na consulta 'sql_command'.
		kwargs: parâmetros das funçoẽs estatísticas usadas no interior dessa função """
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	ranking = list()
	infos = cursor.execute(sql_command.format(*args)).fetchall()
	for info in infos:
		key = func(info[0], **kwargs)
		if key == None:
			continue

		key = [info[0], key]
		ranking.append(key)

	ranking = sorted(ranking, key=lambda ranking: ranking[1], reverse=True)
	conn.close()
	return ranking

def get_id(sql_command, *args):
	conn = sql.connect("py_politica.db")
	cursor = conn.cursor()
	infos = cursor.execute(sql_command.format(*args)).fetchall()
	infos = [i[0] for i in infos]
	return infos