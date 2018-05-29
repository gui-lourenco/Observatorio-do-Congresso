import sqlite3
import aiohttp as aio
from bs4 import BeautifulSoup
import asyncio as asy
import logging
import time
import requests

#Esquemas das tabelas do BD;
#Servem com paramentros para a função insert;
PADRAO_MATERIA = '(id_materia, tipo, autor, numero, data_apresentacao, ementa, apelido, status)'
PADRAO_VOTACAO = '(id_votacao, id_materia, dataHorainicio)'
PADRAO_VOTACAO_S = '(id_votacao, placarSim, placarNao, placarAbs)'
PADRAO_PARLAMENTAR = '(id_parlamentar, nome, casa)'
PADRAO_VOTO = '(id_parlamentar, id_votacao, descricao)'
PADRAO_PARTIDO = '(id_partido, sigla, nome, data_criacao)'
PADRAO_FILIACAO = '(id_parlamentar, id_partido, data_filiacao, data_desfiliacao, uf)'

conn = sqlite3.connect("py_politica.db") #Conexão com o BD;
cursor = conn.cursor() #Criação de um cursor para realizar operações no BD;
logging.getLogger('aiohttp.client').setLevel(logging.ERROR) #Mudança no nivel dos avisos do aiohttp;
tempo_atual = time.localtime() #Data corrente

def main():
	get_partidos()
	loop = asy.get_event_loop()
	loop.run_until_complete(async_materia())
	loop.run_until_complete(async_votacao())
	#loop.run_until_complete(async_filiacao())
	loop.close()

# insere informações no BD
# insert(str, CONST_PADRÃO, tuple)
def insert(tabela, padrao, info):
	columns = ','.join('?'*len(info))
	try:
		cursor.execute('''INSERT INTO {}{} VALUES ({});'''.format(tabela,padrao,columns), info)
		conn.commit()
	except sqlite3.IntegrityError:
		pass

# Função para correção de AttributeError em elementos BeautifulSoup;
# get_text_alt(BeautifulSoup.element, str, arg)
# arg - argumento de tipo variavel
def get_text_alt(elem, tag, alt=None):
	try:
		return elem.find(tag).text
	except AttributeError:
		return alt


def insert_materias(lista_materias):
	for materias in lista_materias:
		for materia in materias.find_all('materia'):
			id_materia = materia.find('codigomateria').text
			tipo = materia.find('siglasubtipomateria').text
			autor = get_text_alt(materia, 'nomeautor', get_text_alt(materia, 'descricaotipoautor'))
			numero = materia.find('numeromateria').text
			data_apresentacao = materia.find('dataapresentacao').text
			ementa = get_text_alt(materia,'ementamateria')
			#temas = get_text_alt(materia, 'indexacaomateria')
			apelido = get_text_alt(materia, 'apelidomateria')
			status = get_text_alt(materia, 'descricaosituacao')
			info = (id_materia, tipo, autor, numero, data_apresentacao, ementa, apelido, status)
			insert('materia', PADRAO_MATERIA, info)

def insert_votacao_secreta(votacao):
	id_votacao = votacao.find('codigosessaovotacao').text
	placar_sim = get_text_alt(votacao, 'totalvotossim')
	placar_nao = get_text_alt(votacao, 'totalvotosnao')
	placar_abs = get_text_alt(votacao, 'totalvotosabstencao')
	info = (id_votacao, placar_sim, placar_nao, placar_abs)
	insert('votacao_secreta', PADRAO_VOTACAO_S, info)

def insert_parlamentar(votacao):
	for parlamentar in votacao.find_all('votoparlamentar'):
		id_parlamentar = parlamentar.find('codigoparlamentar').text
		nome = parlamentar.find('nomeparlamentar').text
		info = (id_parlamentar, nome, 'SF')
		insert('parlamentar', PADRAO_PARLAMENTAR, info)

def insert_voto(votacao):
	id_votacao = votacao.find('codigosessaovotacao').text
	for voto in votacao.find_all('votoparlamentar'):
		id_parlamentar = voto.find('codigoparlamentar').text
		descricao = voto.find('voto').text
		info = (id_parlamentar, id_votacao, descricao)
		insert('voto', PADRAO_VOTO, info)
	
def insert_votacao(lista_votacoes):
	for votacoes in lista_votacoes:
		for votacao in votacoes.find_all('votacao'):
			id_votacao = votacao.find('codigosessaovotacao').text
			id_materia = get_text_alt(votacao,'codigomateria')
			dataHorainicio = votacao.find('datasessao').text + ' ' + votacao.find("horainicio").text + ":00"
			info = (id_votacao, id_materia, dataHorainicio)
			insert('votacao', PADRAO_VOTACAO, info)
			insert_parlamentar(votacao)
			insert_voto(votacao)
			if votacao.find('secreta').text == 'S':
				insert_votacao_secreta(votacao)

def insert_filiacao(filiacoes):
	id_parlamentar = filiacoes.find('codigoparlamentar').text
	uf = get_text_alt(filiacoes,'ufparlamentar')
	for filiacao in filiacoes.find_all('filiacao'):
		id_partido = filiacao.find('codigopartido').text
		data_filiacao = filiacao.find('datafiliacao').text
		data_desfiliacao = get_text_alt(filiacao, 'datadesfiliacao')
		info = (id_parlamentar, id_partido, data_filiacao, data_desfiliacao, uf)
		insert('filiacao', PADRAO_FILIACAO, info)

def format_data(agenda):
	params = list()
	for datas in agenda:
		for data in datas:
			try:
				if 'd' not in data.text:
					params.append(data.text.replace('-',''))		
			except TypeError:
				continue

	return params	

def get_partidos():
	URL = 'http://legis.senado.leg.br/dadosabertos/senador/partidos'
	req = requests.get(URL).text
	partidos = BeautifulSoup(req, 'lxml')
	for partido in partidos.find_all('partido'):
		id_partido = partido.find('codigo').text
		sigla = partido.find('sigla').text
		nome = partido.find('nome').text
		data_criacao = partido.find('datacriacao').text
		info = (id_partido, sigla, nome, data_criacao)
		insert('partido', PADRAO_PARTIDO, info)

async def get_materia(ano, session):
	URL = 'http://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista?ano={}'
	async with session.get(URL.format(ano)) as materias:
		materias = BeautifulSoup(await materias.text(), 'lxml')
		return materias

async def get_agenda(ano, mes, session):
	URL = 'http://legis.senado.leg.br/dadosabertos/plenario/agenda/mes/{}{:02d}01'
	async with session.get(URL.format(ano,mes)) as data:
		data = BeautifulSoup(await data.text(), 'lxml')
		return data.find_all('data')

async def get_votacao(data, session):
	URL = 'http://legis.senado.leg.br/dadosabertos/plenario/lista/votacao/{}'
	async with session.get(URL.format(data)) as votacoes:
		votacoes = BeautifulSoup(await votacoes.text(), 'lxml')
		return votacoes

async def get_filiacao(parlamentar, session):
	URL = 'http://legis.senado.leg.br/dadosabertos/senador/{}/filiacoes'
	async with session.get(URL.format(parlamentar))as filiacoes:
		filiacoes = BeautifulSoup(await filiacoes.text(), 'lxml')
		return filiacoes

async def async_materia():
	async with aio.ClientSession(trust_env = True) as session:
		materias = [get_materia(ano, session) for ano in range(2010, tempo_atual[0])]
		materias = await asy.gather(*materias)
		insert_materias(materias)

async def async_votacao():
	async with aio.ClientSession(trust_env = True) as session:
		agenda = [get_agenda(ano, mes, session) for ano in range(2010, tempo_atual[0]) for mes in range(1,13)]
		agenda = await asy.gather(*agenda)
		agenda = format_data(agenda)
		votacao1 = [get_votacao(data, session) for data in agenda[:500]]
		votacao2 = [get_votacao(data, session) for data in agenda[500:1000]]
		votacao3 = [get_votacao(data, session) for data in agenda[1000:1500]]
		votacao4 = [get_votacao(data, session) for data in agenda[1500:2000]]
		votacao5 = [get_votacao(data, session) for data in agenda[2000:]]
		for votacao in [votacao1, votacao2, votacao3, votacao4, votacao5]:
			voto = await asy.gather(*votacao)
			insert_votacao(voto)


async def async_filiacao():
	async with aio.ClientSession(trust_env = True) as session:
		sql_command = '''
			SELECT id_parlamentar
			FROM parlamentar
		'''
		parlamentares = cursor.execute(sql_command).fetchall()
		parlamentares = [parlamentar[0] for parlamentar in parlamentares]
		filiacoes1 = [get_filiacao(parlamentar, session) for parlamentar in parlamentares[:100]]
		filiacoes2 = [get_filiacao(parlamentar, session) for parlamentar in parlamentares[100:]]
		for filia in [filiacoes1, filiacoes2]:
			filia = await asy.gather(*filia)
			for filiacao in filia:
				insert_filiacao(filiacao)

main()
conn.close()