import basedosdados as bd
import pandas as pd
import os
from typing import List


def query_total_pop_by_sex_age_2022(mun_id: int, project_id: str) -> pd.DataFrame:
    
    # carregar faixas etarias censo 2000.csv, e utilizar no where para escolher somentes as linhas de interesse
    # conferir pq para o mesmo município aparecem duas vezes a mesma faixa etária no mesmo sexo
     	 
	'''
	Returns a Pandas Dataframe with the sum of sample weights from the 2010 census that represent 
	population of a given Brazilian municipality, grouped by sex and age.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	____________________________________________________________________________________________________________________
	FUTURES

	File for age group is hardcoded in the function. In future versions, it should be given by the user as a argument.

	'''


	current_dir = os.path.dirname(__file__)

	csv_file_path = os.path.join(current_dir, 'source/tab/faixas_etarias_censo_2022.csv')

	faixa_etaria = pd.read_csv(csv_file_path, sep=';')

	query = f"""
				SELECT  
						sexo AS Sexo, 
						grupo_idade AS Idade,
						SUM(populacao_residente) AS Pop 
				FROM 
						`basedosdados.br_ibge_censo_2022.populacao_residente_municipio` 
				WHERE 
						(id_municipio = '{mun_id}')
						AND
						(grupo_idade IN {tuple(faixa_etaria['faixa_2000_original'].unique())})
				GROUP BY
						sexo, grupo_idade            
				ORDER BY 
						sexo, grupo_idade;
				"""
	df = bd.read_sql(query=query,billing_project_id=project_id).fillna(0)

	df['Sexo'] = df['Sexo'].map({'Homens':'Masculino', 'Mulheres':'Feminino'})
	
	try:
		return df
	except Exception as e:
		print(f'Something went wrong! {e}')



def query_total_pop_by_sex_age_2010(mun_id: int, project_id: str) -> pd.DataFrame:
	'''
	Returns a Pandas Dataframe with the sum of sample weights from the 2010 census that represent 
	population of a given Brazilian municipality, grouped by sex and age.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	'''

	query = f"""
				SELECT  
						v0601 AS Sexo, 
						v6036 AS Idade,
						SUM(peso_amostral) AS Peso 
				FROM 
						`basedosdados.br_ibge_censo_demografico.microdados_pessoa_2010` 
				WHERE 
						id_municipio = '{mun_id}'
				GROUP BY 
						v0601, v6036
				ORDER BY 
						v0601, v6036;
				"""
	df = bd.read_sql(query=query,billing_project_id=project_id)
	df['Sexo'] = df['Sexo'].map({'1':'Masculino', '2':'Feminino'})

	try:
		return df
	except Exception as e:
		print(f'Something went wrong! {e}')


def query_total_pop_by_sex_age_2000(mun_id: int, project_id: str) -> pd.DataFrame:
	'''
	Returns a Pandas Dataframe with the sum of sample weights from the 2000 census that represent 
	population of a given Brazilian municipality, grouped by sex and age.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	'''

	query = f"""
				SELECT 
						v0401 AS Sexo, 
						v4752 AS Idade,
						SUM(p001) AS Peso 
				FROM 
						`basedosdados.br_ibge_censo_demografico.microdados_pessoa_2000` 
				WHERE 
						id_municipio = '{mun_id}'
				GROUP BY 
						v0401, v4752
				ORDER BY 
						v0401, v4752;
				"""

	df = bd.read_sql(query=query,billing_project_id=project_id)
	df['Sexo'] = df['Sexo'].map({'1':'Masculino', '2':'Feminino'})

	try:
		return df
	except Exception as e:
		print(f'Something went wrong! {e}')
        


def standard_age_groups(df: pd.DataFrame, age_group_csv_path: str, year: int) -> pd.DataFrame:
	'''
	Takes the resulting DataFrame from migration queries and returns standardized age groups according to a given csv which maps 
	ages and age groups.

	CSV columns must be separated by semi-colon.   
	'''

	df_age_group = pd.read_csv(age_group_csv_path, sep=';') #loads csv which maps ages and age groups
	dict_age_group = {tup[1]:tup[2] for tup in df_age_group.itertuples()} # generates dictionary that maps age to age group 
	all_age_groups = df_age_group['Faixa Etária'].unique()
      
        
	df['Faixa Etária'] = df.Idade.map(dict_age_group) #retrieves specific age group for each age record in the dataframe
	df = df.drop(columns='Idade').groupby(by=['Sexo', 'Faixa Etária'], as_index=False).sum() #groups data by sex and age group
	df.set_index(['Sexo', 'Faixa Etária'], inplace=True) #sets index
	new_index = pd.MultiIndex.from_product([df.index.levels[0], all_age_groups], names=['Sexo', 'Faixa Etária']) #generates standardized index
	df = df.reindex(new_index) #expands index with possible missing categories
	if year in [2010, 2000]:
		df.rename(columns={'Peso':'Pop'}, inplace=True)
	df['Pop'].fillna(0, inplace=True) #fills nan values in the weight column with 0
	df.reset_index(inplace=True) # reset index    
	df['Ano'] = year
	df['Faixa Etária'] = pd.Categorical(df['Faixa Etária'], categories=all_age_groups, ordered=True) # makes Faixa Etária become ordered categorical
	df = df.sort_values(['Sexo', 'Faixa Etária']).set_index(['Ano','Sexo', 'Faixa Etária']) # sort sex and age group and usem them as final index
	if year in [2010, 2000]:
		df = df.round(decimals=0).astype(int)


	return df

def concatenate_treated_dfs(dfs: List[pd.DataFrame]):
    """
    Takes dataframes that were treated by standard_age_groups function and concatenates them.
    
    """
    df = pd.concat(objs=dfs, ignore_index=False)
    df = df.reset_index().pivot_table(columns='Ano', index=['Sexo', 'Faixa Etária'], values=['Pop'])
    df = df.astype(int)
    df = df.droplevel(level=0, axis=1)
    df.columns.name = None

    return df


########
######## THE CODE BELOW IS NOT IN USE IN THE NOTEBOOKS SO FAR. A TABLE, moradores_dppo.csv, WAS ALREADY GENERATED FOR ALL MUNICIPALITIES IN BRAZIL
######## 
########


def query_dppo_2022(mun_id: int, project_id: str) -> pd.DataFrame:
    
    # carregar faixas etarias censo 2000.csv, e utilizar no where para escolher somentes as linhas de interesse
    # conferir pq para o mesmo município aparecem duas vezes a mesma faixa etária no mesmo sexo
     	 
	'''
	Returns a Pandas Dataframe with the number of household dwellers from the 2022 census.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	'''


	current_dir = os.path.dirname(__file__)

	csv_file_path = os.path.join(current_dir, 'source/tab/faixas_etarias_censo_2022.csv')

	faixa_etaria = pd.read_csv(csv_file_path, sep=';')

	query = f"""
				SELECT  
						moradores AS moradores_dppo_2022, 
						domicilios AS dppo_2022

				FROM 
						`basedosdados.br_ibge_censo_2022.domicilio_morador_municipio` 
				WHERE 
						(id_municipio = '{mun_id}')
						            
				ORDER BY 
						id_municipio;
				"""
	df = bd.read_sql(query=query,billing_project_id=project_id)

	df['moradores/dppo 2022'] = (df['moradores_dppo_2022'] / df['dppo_2022']).round(2)
	
	try:
		return df
	except Exception as e:
		print(f'Something went wrong! {e}')


def query_household_residents_2010(mun_id: int, project_id: str) -> pd.DataFrame:
	'''
	Returns a Pandas Dataframe with the number of household dwellers from the 2010 census.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	'''

	query = f"""
				SELECT  
						SUM(peso_amostral) AS Peso 
				FROM 
						`basedosdados.br_ibge_censo_demografico.microdados_pessoa_2010` 
				WHERE 
						id_municipio = '{mun_id}';
				"""
	df = bd.read_sql(query=query,billing_project_id=project_id)

	try:
		return df
	except Exception as e:
		print(f'Something went wrong! {e}')

