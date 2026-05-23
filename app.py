import streamlit as st
from streamlit_gsheets import GSheetsConnection
from groq import Groq
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

st.set_page_config(page_title="Icesi Writing Lab", layout="wide")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    client = Groq(api_key=st.secrets["api_keys"]["groq"])
except Exception as e:
    st.error(f"Error de configuración/conexión: {e}")

if 'spark' not in st.session_state:
    try:
        st.session_state.spark = SparkSession.builder \
            .appName("IcesiWritingLabAnalytics") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
    except Exception as e:
        st.error(f"Error de infraestructura: No se pudo conectar al clúster de Spark: {e}")

spark = st.session_state.get('spark', None)

# CRONOGRAMA DE INVESTIGACIÓN
st.sidebar.title("Research Timeline")
week_selection = st.sidebar.selectbox("Current Research Week:", [
    "Week 2: Introduction", 
    "Week 3: Grammar Implementation", 
    "Week 4: Vocabulary Implementation",
    "Week 5: Cohesion Implementation",
    "Week 6: Argumentation Implementation",
    "Week 7: Revision Implementation",
    "Week 8: Final Task"
])

role = st.sidebar.radio("Role:", ["Student", "Admin"])

# INTERFAZ DE ESTUDIANTE
if role == "Student":
    st.header("Module 1: Cities, Towns & Villages")
    st.info("General Objective: Plan and perform a short oral presentation about a city/town.")

    if 'ai_response' not in st.session_state:
        st.session_state.ai_response = ""

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("1. Writing Area")
        student_id = st.text_input("Anonymous ID (A00XXXXX):")
        draft = st.text_area("Paste your writing draft here:", height=300)

    with col2:
        st.subheader("2. Select Your Strategy")
        strategy = st.selectbox("What do you want to focus on?", [
            "Grammar: Verb Tenses & Subject-Verb Agreement", 
            "Vocabulary: City Life, Geography & Collocations", 
            "Cohesion: Transitions & Paragraph Unity",
            "Structure: Thesis Statements & Reasoning",
            "Editing: Self-Revision Strategies"
        ])

        if st.button("Consult AI Tutor"):
            if student_id and draft:
                sys_msg = f"""
                You are a Level II English Tutor at Icesi University. 
                Focus: {strategy} within the context of describing/comparing cities.
                Protocol: Compare-and-Explain (CEP). 

                STRICT INSTRUCTIONS:
                1. Identify max 3 specific areas for improvement.
                2. Use the format: "Fragment" --> "Hint".
                3. Do NOT provide the correct answer, only pedagogical clues to trigger reflection.
                4. Keep explanations extremely brief.
                """
                try:
                    res = client.chat.completions.create(
                        messages=[{"role": "system", "content": sys_msg}, {"role": "user", "content": draft}],
                        model="llama-3.3-70b-versatile"
                    )
                    st.session_state.ai_response = res.choices[0].message.content
                except Exception as e:
                    st.error(f"AI Error: {e}")
            else:
                st.warning("Please provide your ID and writing draft.")

    # Muestra de Feedback Estructurado
    if st.session_state.ai_response:
        st.subheader("AI Tutor Observations")
        st.info(st.session_state.ai_response)
        st.divider()

        # --- REFLEXIÓN Y GUARDADO ---
        st.subheader("3. Student Reflection (CEP)")
        reflection = st.text_area("Why did you decide to follow or ignore this feedback?")

        if st.button("Submit Interaction :)"):
            if reflection and student_id:
                try:
                    draft_word_count = 0
                    reflection_word_count = 0

                    # Procesamiento con Apache Spark distribuido
                    if spark:
                        with st.spinner("Processing metadata using Apache Spark..."):
                            # 1. Creamos el DataFrame en el clúster
                            data_spark = [(draft.strip(), reflection.strip())]
                            columns_spark = ["draft_text", "reflection_text"]
                            spark_df = spark.createDataFrame(data_spark, columns_spark)
                            
                            # 2. Spark calcula las métricas de longitud limpiando espacios duplicados
                            analytics_df = spark_df.selectExpr(
                                "size(split(regexp_replace(draft_text, ' {2,}', ' '), ' ')) as draft_words",
                                "size(split(regexp_replace(reflection_text, ' {2,}', ' '), ' ')) as reflection_words"
                            ).collect()
                            
                            # Extraemos transformando explícitamente a tipo int nativo de Python
                            draft_word_count = int(analytics_df[0]["draft_words"])
                            reflection_word_count = int(analytics_df[0]["reflection_words"])
                    else:
                        # Fallback seguro en caso de que Spark no esté inicializado
                        draft_word_count = len(draft.split())
                        reflection_word_count = len(reflection.split())

                    # 3. Guardado tradicional en Google Sheets
                    existing = conn.read(worksheet="Sheet1", ttl=0)

                    new_row = pd.DataFrame([{
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "Week": week_selection, 
                        "Student_ID": student_id,
                        "Strategy": strategy,
                        "Draft": draft,
                        "Draft_Word_Count": draft_word_count,
                        "AI_Feedback": st.session_state.ai_response,
                        "Reflection": reflection,
                        "Reflection_Word_Count": reflection_word_count
                    }])

                    updated = pd.concat([existing, new_row], ignore_index=True)
                    conn.update(worksheet="Sheet1", data=updated)

                    st.success(f"🎉 Synced! Data saved. Spark verified {draft_word_count} words in draft and {reflection_word_count} in reflection.")
                    st.session_state.ai_response = "" 
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Sync/Spark Error: {e}. Check container logs or cloud sheet permissions.")
            else:
                st.warning("Please fill in your ID and the reflection field.")

# --- ADMIN VIEW ---
else:
    st.header("Researcher Dashboard")
    pwd = st.text_input("Password:", type="password")
    if pwd == "Icesi2026*":
        try:
            data = conn.read(worksheet="Sheet1", ttl=0)
            st.write("### Real-time Logs")
            st.dataframe(data)
            st.download_button(
                label="Download CSV for Data Cleaning (Week 9)",
                data=data.to_csv(index=False).encode('utf-8'),
                file_name="research_data_icesi.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.warning(f"Could not read Sheet1: {e}")