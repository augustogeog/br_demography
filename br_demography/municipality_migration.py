import basedosdados as bd
import pandas as pd


### query_total_moradores_mun future -> improve try except statement to catch specific common erros and return adapted messages
### query_total_moradores_mun future -> Make it work not just for 2010, but 2022, maybe 2000 too.
def query_total_population(mun_id: int, project_id: str) -> pd.DataFrame:
    '''
    Returns a Pandas Dataframe with sum of sample weights from the 2010 census which will represent 
    the total number of inhabitants for a given Brazilian municipality according to its id.

    Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.
    '''

    query = f"""SELECT SUM(peso_amostral) 
            FROM `basedosdados.br_ibge_censo_demografico.microdados_pessoa_2010` WHERE id_municipio = '{mun_id}'
            """
    try:
        return bd.read_sql(query=query,billing_project_id=project_id)
    except Exception as e:
        print(f'Something went wrong! {e}')



def query_emigration_by_sex_age(mun_id: int, project_id: str) -> pd.DataFrame:
    '''
    Returns a Pandas Dataframe with the sum of sample weights from the 2010 census that represent people that left
    a given Brazilian municipality since 31/07/2005, grouped by sex and age.

    Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.
    
    '''

    query = f"""
                SELECT 
                        SUM(peso_amostral) AS Peso, 
                        v0601 AS Sexo, 
                        v6036 AS Idade 
                FROM 
                        `basedosdados.br_ibge_censo_demografico.microdados_pessoa_2010` 
                WHERE 
                        v6264 = '{mun_id}'
                GROUP BY 
                        v0601, v6036
                ORDER BY 
                        v0601, v6036;
                """
    try:
        return bd.read_sql(query=query,billing_project_id=project_id)
    except Exception as e:
        print(f'Something went wrong! {e}')
        


def query_immigration_by_sex_age(mun_id: int, project_id: str) -> pd.DataFrame:
	'''
	Returns a Pandas Dataframe with the sum of sample weights from the 2010 census that represent people that arrived at
	a given Brazilian municipality since 31/07/2005, grouped by sex and age.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	'''

	query = f"""
			SELECT 
					SUM(peso_amostral) AS Peso, 
					v0601 AS Sexo, 
					v6036 AS Idade 
			FROM 
					`basedosdados.br_ibge_censo_demografico.microdados_pessoa_2010` 
			WHERE 
					id_municipio = '{mun_id}' AND v6264 IS NOT NULL
			GROUP BY 
					v0601, v6036
			ORDER BY 
					v0601, v6036;
			"""
	try:
		return bd.read_sql(query=query, billing_project_id=project_id)
	except Exception as e:
		print(f'Something went wrong! {e}')



def query_emigration_by_sex_age_2000(mun_id: int, project_id: str) -> pd.DataFrame:
    '''
    Returns a Pandas Dataframe with the sum of sample weights from the 2010 census that represent people that left
    a given Brazilian municipality since 31/07/1995, grouped by sex and age.

    Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.
    
    '''

    query = f"""
                SELECT 
                        SUM(p001) AS Peso, 
                        v0401 AS Sexo, 
                        v4752 AS Idade 
                FROM 
                        `basedosdados.br_ibge_censo_demografico.microdados_pessoa_2000` 
                WHERE 
                        v4250 = '{mun_id}'
                GROUP BY 
                        v0401, v4752
                ORDER BY 
                        v0401, v4752;
                """
    try:
        return bd.read_sql(query=query,billing_project_id=project_id)
    except Exception as e:
        print(f'Something went wrong! {e}')



def query_immigration_by_sex_age_2000(mun_id: int, project_id: str) -> pd.DataFrame:
	'''
	Returns a Pandas Dataframe with the sum of sample weights from the 2000 census that represent people that arrived at
	a given Brazilian municipality since 31/07/1995, grouped by sex and age.

	Requires project_id, the Google Cloud project id for billing, and mun_id, the seven-figures municipality id.

	'''

	query = f"""
			SELECT 
					SUM(p001) AS Peso, 
					v0401 AS Sexo, 
					v4752 AS Idade 
			FROM 
					`basedosdados.br_ibge_censo_demografico.microdados_pessoa_2000` 
			WHERE 
					id_municipio = '{mun_id}' AND v0424 IN ('3','4')
			GROUP BY 
					v0401, v4752
			ORDER BY 
					v0401, v4752;
			"""
	try:
		return bd.read_sql(query=query, billing_project_id=project_id)
	except Exception as e:
		print(f'Something went wrong! {e}')


    

def standard_age_groups(df: pd.DataFrame, age_group_csv_path: str) -> pd.DataFrame:
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
	df['Peso'].fillna(0, inplace=True) #fills nan values in the weight column with 0
	df.reset_index(inplace=True) # reset index
	df['Faixa Etária'] = pd.Categorical(df['Faixa Etária'], categories=all_age_groups, ordered=True) # makes Faixa Etária become ordered categorical
	df['Sexo'] = df.Sexo.map({'1':'Masculino', '2':'Feminino'})
	df = df.sort_values(['Sexo', 'Faixa Etária']).set_index(['Sexo', 'Faixa Etária']) # sort sex and age group and usem them as final index
		
	return df



