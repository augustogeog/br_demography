import basedosdados as bd
import pandas as pd


def query_deaths(mun_id: int, project_id: str, start_year=2002, end_year=2022) -> pd.DataFrame:
    '''
    Returns a Pandas Dataframe with deaths microdata from "Ministério da Saúde".

    Requires:
        -> project_id, the Google Cloud project id for billing;
        -> mun_id, the seven-figures municipality id;
        -> start_year, the first year for the beginning of the series
        -> end_year, the last year for the beginning of the series
    '''

    if not isinstance(mun_id, int):
        raise ValueError("mun_id should be an integer.")
    if not isinstance(start_year, int) or not isinstance(end_year, int):
        raise ValueError("start_year and end_year should be integers.")
    if start_year > end_year:
        raise ValueError("start_year cannot be greater than end_year.")


    query = f"""
            SELECT 
                ano as Ano,
                sexo as Sexo,
                idade as Idade,
            FROM 
                `basedosdados.br_ms_sim.microdados`
            WHERE
                (id_municipio_residencia = '{mun_id}')
                AND
                (ano BETWEEN {start_year} AND {end_year})
                AND
                (tipo_obito = '2')
                
            ORDER BY
                ano, sexo, idade;
            """
    try:
        return bd.read_sql(query=query,billing_project_id=project_id)
    except Exception as e:
        print(f'Something went wrong! {e}')


#def standard_age_groups(age_group_csv_path: str) -> pd.DataFrame:
def standard_age_groups(df: pd.DataFrame, age_group_csv_path: str) -> pd.DataFrame:
    '''
    Takes the resulting DataFrame from deaths queries and returns standardized age groups according to a given csv which maps 
    ages and age groups.

    CSV columns must be separated by semi-colon.   
    '''

    df_age_group = pd.read_csv(age_group_csv_path, sep=';') #loads csv which maps ages and age groups
    dict_age_group = {tup[1]:tup[2] for tup in df_age_group.itertuples()} # generates dictionary that maps age to age group 
    all_age_groups = df_age_group['Faixa Etária'].unique()
    

    df.Idade.fillna(value=int(df.Idade.mean()), inplace=True)
    
    df['Faixa Etária'] = df.Idade.map(dict_age_group) #retrieves specific age group for each age record in the dataframe

    df['Óbitos'] = 1

    df = df.drop(columns='Idade').groupby(by=['Ano', 'Sexo', 'Faixa Etária'], as_index=True).sum() #groups data by sex and age group
    new_index = pd.MultiIndex.from_product([df.index.levels[0], df.index.levels[1], all_age_groups], names=['Ano', 'Sexo', 'Faixa Etária']) #generates standardized index
    df = df.reindex(new_index) #expands index with possible missing categories
    df['Óbitos'].fillna(0, inplace=True) #fills nan values in the weight column with 0
    df.reset_index(inplace=True) # reset index
    df['Faixa Etária'] = pd.Categorical(df['Faixa Etária'], categories=all_age_groups, ordered=True) # makes Faixa Etária become ordered categorical
    df['Sexo'] = df.Sexo.map({'1':'Masculino', '2':'Feminino'})
    df = df.set_index(['Ano', 'Sexo', 'Faixa Etária']).sort_index().astype(int) # sort sex and age group and usem them as final index
    df = df.reset_index().pivot_table(columns=['Ano'], index=['Sexo', 'Faixa Etária'], values='Óbitos')

    return df