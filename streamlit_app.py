import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()


st.title("LinkedIn Job Analytics Dashboard")

# ===============================
# 1. Top 10 jobs par industrie
# ===============================
st.header("Top 10 des titres de postes les plus publiés par industrie")


# ----------------------------
# Requête SQL avec filtre spécialités > 10 titres
# ----------------------------
query_top_titles = """
SELECT *
FROM (
    SELECT 
        cs.speciality,
        j.title,
        COUNT(*) AS nb_jobs,
        ROW_NUMBER() OVER (
            PARTITION BY cs.speciality 
            ORDER BY COUNT(*) DESC
        ) AS rank
    FROM LINKEDIN.GOLD.job_postings j
    JOIN LINKEDIN.SILVER.company_specialities cs 
        ON j.company_id = cs.company_id
    GROUP BY cs.speciality, j.title
) t
WHERE rank <= 10
  AND speciality IN (
      SELECT speciality
      FROM LINKEDIN.GOLD.job_postings j
      JOIN LINKEDIN.SILVER.company_specialities cs 
          ON j.company_id = cs.company_id
      GROUP BY speciality
      HAVING COUNT(DISTINCT title) > 10
  )
ORDER BY speciality, rank;
"""

# Exécution de la requête avec Snowpark et conversion en pandas
df_titles = session.sql(query_top_titles).to_pandas()

# ----------------------------
# Filtrage interactif par spécialité
# ----------------------------
specialities = df_titles["SPECIALITY"].unique()
selected_speciality = st.selectbox("Choisir une spécialité", specialities)

df_filtered = df_titles[df_titles["SPECIALITY"] == selected_speciality]

# ----------------------------
# Graphique Altair
# ----------------------------
chart_titles = alt.Chart(df_filtered).mark_bar().encode(
    x=alt.X('NB_JOBS:Q', title='Nombre d’offres'),
    y=alt.Y('TITLE:N', sort='-x', title='Titre du poste'),
    tooltip=['TITLE', 'NB_JOBS']
).properties(
    width=700,
    height=400,
    title=f"Top 10 des titres pour {selected_speciality}"
)

st.altair_chart(chart_titles)


# ===============================
# 2. Top salaires par industrie
# ===============================
st.header("Top 10 des postes les mieux rémunérés par industrie")


# ----------------------------
# Requête SQL avec filtre spécialités > 10 titres
# ----------------------------
query_top_salary = """
SELECT *
FROM (
    SELECT 
        cs.speciality,
        jp.title,
        MAX(jp.max_salary) AS max_salary,
        ROW_NUMBER() OVER (
            PARTITION BY cs.speciality 
            ORDER BY MAX(jp.max_salary) DESC
        ) AS rank
    FROM LINKEDIN.GOLD.job_postings jp
    JOIN LINKEDIN.SILVER.company_specialities cs 
        ON jp.company_id = cs.company_id
    WHERE jp.max_salary IS NOT NULL
    GROUP BY cs.speciality, jp.title
) t
WHERE rank <= 10
  AND speciality IN (
      SELECT speciality
      FROM LINKEDIN.GOLD.job_postings jp
      JOIN LINKEDIN.SILVER.company_specialities cs 
          ON jp.company_id = cs.company_id
      GROUP BY speciality
      HAVING COUNT(DISTINCT title) > 10
  )
ORDER BY speciality, rank;
"""

# Exécution de la requête avec Snowpark et conversion en pandas
df_salary = session.sql(query_top_salary).to_pandas()

# ----------------------------
# Filtrage interactif par spécialité
# ----------------------------
specialities_salary = df_salary["SPECIALITY"].unique()
selected_speciality_salary = st.selectbox("Choisir une spécialité (salaires)", specialities_salary)

df_filtered_salary = df_salary[df_salary["SPECIALITY"] == selected_speciality_salary]

# ----------------------------
# Graphique Altair
# ----------------------------
chart_salary = alt.Chart(df_filtered_salary).mark_bar().encode(
    x=alt.X('MAX_SALARY:Q', title='Salaire maximum'),
    y=alt.Y('TITLE:N', sort='-x', title='Titre du poste'),
    tooltip=['TITLE', 'MAX_SALARY']
).properties(
    width=700,
    height=400,
    title=f"Top 10 des postes les mieux payés pour {selected_speciality_salary}"
)

st.altair_chart(chart_salary)

# ===============================
# 3. Répartition par taille d’entreprise
# ===============================
st.header("Répartition des offres par taille d’entreprise")

query_company_size = """
SELECT 
    CASE 
        WHEN c.company_size <= 2 THEN 'small_size'
        WHEN c.company_size <= 4 THEN 'medium_size'
        WHEN c.company_size <= 7 THEN 'big_size'
        ELSE 'unknown'
    END AS company_size_group,
    
    COUNT(*) AS nb_jobs,
    
    ROUND(
        100 * COUNT(*) / SUM(COUNT(*)) OVER (), 
        2
    ) AS pct

FROM LINKEDIN.GOLD.job_postings j
JOIN LINKEDIN.SILVER.companies c 
    ON j.company_id = c.company_id

GROUP BY company_size_group
ORDER BY company_size_group;
"""

df_size = session.sql(query_company_size).to_pandas()

st.bar_chart(df_size.set_index('COMPANY_SIZE_GROUP')['NB_JOBS'])


# ===============================
# 4. Répartition par secteur
# ===============================
st.header("Répartition des offres d’emploi par secteur d’activité")

query_sector = """
SELECT 
    cs.speciality,
    COUNT(j.job_id) AS nb_jobs
FROM LINKEDIN.GOLD.job_postings j
JOIN LINKEDIN.SILVER.companies c 
    ON j.company_id = c.company_id
JOIN LINKEDIN.SILVER.Company_specialities cs 
    ON c.company_id = cs.company_id
GROUP BY cs.speciality
ORDER BY nb_jobs DESC
LIMIT 12;
"""

df_sector = session.sql(query_sector).to_pandas()

st.bar_chart(df_sector.set_index('SPECIALITY')['NB_JOBS'])


# ===============================
# 5. Répartition par type d’emploi
# ===============================
st.header("Répartition des offres d’emploi par type d’emploi")

query5 = """
SELECT 
    formatted_work_type, 
    COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.job_postings
GROUP BY formatted_work_type
ORDER BY nb_jobs DESC
"""

df5 = session.sql(query5).to_pandas()

# Optionnel : rendre plus lisible
df5["FORMATTED_WORK_TYPE"] = df5["FORMATTED_WORK_TYPE"].replace({
    "FULL_TIME": "Temps plein",
    "PART_TIME": "Temps partiel",
    "INTERNSHIP": "Stage"
})

st.bar_chart(df5.set_index("FORMATTED_WORK_TYPE")["NB_JOBS"])