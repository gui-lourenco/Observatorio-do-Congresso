'''Versão 1.0.1'''
import sqlite3 as sql

con = sql.connect('py_politica.db')

con.cursor().executescript('''
	CREATE TABLE partido( 
		id_partido varchar primary key,
		sigla varchar not null,
		nome varchar not null,
		data_criacao date not null);

	CREATE TABLE parlamentar( 
		id_parlamentar varchar primary key,
		nome varchar not null,
		casa char not null);

	CREATE TABLE filiacao( 
		id_parlamentar varchar not null,
		id_partido varchar not null,
		data_filiacao date not null,
		data_desfiliacao date null,
		uf varchar not null,
		primary key(id_parlamentar, id_partido, data_filiacao),
		foreign key(id_parlamentar) references parlamentar(id_parlamentar),
		foreign key(id_partido) references partido(id_partido));

	CREATE TABLE materia( 
		id_materia varchar primary key,
		tipo varchar null,
		autor varchar null,
		numero integer null,
		data_apresentacao date null,
		ementa varchar null,
		apelido varchar null,
		status varchar null);
	
	CREATE TABLE temas_materia(
		id_materia varchar not null,
		tema varchar not null,
		primary key (id_materia, tema),
		foreign key (id_materia) references materia (id_materia));
	
	CREATE TABLE votacao( 
		id_votacao varchar primary key,
		id_materia varchar null,
		dataHoraInicio datetime not null,
		foreign key(id_materia) references materia(id_materia));

	CREATE TABLE votacao_secreta( 
		id_votacao varchar not null,
		placarSim integer null,
		placarNao integer null,
		placarAbs integer null,
		primary key(id_votacao),
		foreign key(id_votacao) references votacao(id_votacao));
		
	CREATE TABLE voto( 
		id_parlamentar varchar not null,
		id_votacao varchar not null,
		descricao integer not null,
		primary key(id_parlamentar, id_votacao),
		foreign key (id_votacao) references votacao(id_votacao),
		foreign key (id_parlamentar) references parlamentar(id_parlamentar));''')

con.commit()
con.close()