from sqlparse import format
import psycopg2
import pandas

def generateSQL(kpi_name: str, group_ids: str, tx: str, date_range: str) -> str:
    if kpi_name not in ['mentions','conv']: raise ValueError("kpi name can only have 'mentions' and 'conv' as values")
    group_ids = tuple(group_ids)
    queries = {
        'mentions':f"""Select groupid, createdatutc,textlower, sentimentrvalue,sourceid,parentsourceid,type from bd_cs_prod_conversations
                    where groupid in {group_ids} and ({tx}) and ({date_range})""",

        'conv':f"""Select groupid,createdatutc,textlower,sentimentrvalue,sourceid,parentsourceid,type
                        from 
                        bd_cs_prod_conversations 
                        where 
                        groupid in {group_ids} and ({tx}) and ({date_range})

                        union

                        Select groupid,createdatutc,textlower,sentimentrvalue,sourceid,parentsourceid,type
                        from bd_cs_prod_conversations 
                        where parentsourceid IN (
                        Select sourceid
                        from 
                        bd_cs_prod_conversations 
                        where 
                        groupid in {group_ids}
                        and
                        (
                        {tx}
                        ) 
                        and (type = 'Post') 
                        and (grouptype = 'Facebook') 
                        and 
                        ( {date_range}
                        )
                        )
                        and (type = 'Comment') and ({date_range})""",
    }
    return format(queries[kpi_name], reindent=True, keyword_case='upper')

def executeQuery(dbanme: str, host: str, port: int, user: str, password: str, query: str)-> pandas.DataFrame:
    with psycopg2.connect(
        dbname=dbanme,
        host=host,
        port=port,
        user=user,
        password=password) as connection:
        cur1 = connection.cursor()
        cur1.execute(query)
        output = cur1.fetchall()
    connection.close()
    try: 
        output_df = pandas.DataFrame(output)
        output_df.columns = ['groupid','createdatutc','textlower','senti','sourceid','parentsourceid','type']
    except ValueError:
        output_df = pandas.DataFrame(columns=['groupid','createdatutc','textlower','senti','sourceid','parentsourceid','type'])
    return output_df