import pandas as pd
import operator
from openai import OpenAI
import os
from dotenv import load_dotenv
import json
import requests
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
TYPEFORM_API_KEY = os.getenv("TYPEFORM_API_KEY")
base_url = "https://api.typeform.com"

class PreProcess:
    """Pre-process CSV and perform various operations"""
    def __init__(self, filename, group_filter=None, **filters):
        print("start")
        df = pd.read_csv(filename)
        
        # Drop unwanted columns
        if "Network ID" in df.columns:
            df.drop(columns=["Network ID"], inplace=True)

        # Trim column names to avoid trailing spaces
        df.columns = df.columns.str.strip()

        # Define valid operators
        ops = {
            "=": operator.eq,
            "!=": operator.ne,
            ">=": operator.ge,
            "<=": operator.le,
            ">": operator.gt,
            "<": operator.lt
        }

        # Apply filtering based on provided arguments
        for key, value in filters.items():
            parts = key.split(" ")
            if len(parts) == 2:
                col, op = parts
                if col in df.columns and op in ops:
                    df = df[ops[op](df[col], value)]
            else:
                raise ValueError(f"Invalid filter format: {key}. Expected 'column operator value'.")
        print('filters applied')

        # 🔹 **Find Respondents in Selected Group Before Pivoting**
        if group_filter:
            question = group_filter.get("question")
            group = group_filter.get("group")

            # Validate question
            if question not in df.columns:
                raise ValueError(f"Question '{question}' not found in original dataset.")

            # Validate group
            if group not in ["Low", "Mod", "High"]:
                raise ValueError("Invalid group. Must be 'Low', 'Mod', or 'High'.")

            # Melt dataset to long format for group filtering
            df_melt = df.melt(id_vars=["#"], var_name="Question", value_name="Answer")
            df_melt['Answer'] = pd.to_numeric(df_melt['Answer'], errors='coerce')
            df_melt.dropna(inplace=True)
            df_melt["Answer"] = df_melt["Answer"].astype(int)

            # Identify respondents who belong to the selected group
            if group == "Low":
                respondent_ids = df_melt[
                    (df_melt["Question"] == question) & (df_melt["Answer"].between(0, 6))
                ]["#"].unique()
            elif group == "Mod":
                respondent_ids = df_melt[
                    (df_melt["Question"] == question) & (df_melt["Answer"].between(7, 8))
                ]["#"].unique()
            elif group == "High":
                respondent_ids = df_melt[
                    (df_melt["Question"] == question) & (df_melt["Answer"].between(9, 10))
                ]["#"].unique()
        else:
            respondent_ids = df["#"].unique() 

        # Filter dataset based on selected respondents
        df = df[df["#"].isin(respondent_ids)]

        # 🔹 **Proceed with Normal Transformation After Filtering**
        df_melt = df.melt(id_vars=["#"], var_name="Question", value_name="Answer")
        df_melt['Answer'] = pd.to_numeric(df_melt['Answer'], errors='coerce')
        df_melt.dropna(inplace=True)
        df_melt["Answer"] = df_melt["Answer"].astype(int)
        print("melt done")
        self.df_melt = df_melt  # Already filtered based on respondent_ids

    def count_data(self):
        try:
            df_pivot = self.df_melt.pivot_table(index='Question', columns='Answer', aggfunc='size', fill_value=0)
            df_pivot = df_pivot.reindex(columns=list(range(0, 11)), fill_value=0)
            df_pivot.reset_index(inplace=True)
            print("pivot done")

            # Compute final categorical percentages
            total_counts = df_pivot.loc[:, range(0, 11)].sum(axis=1).replace(0, 1)
            df_pivot["STD"] = df_pivot.loc[:, range(0, 11)].std(axis=1).round(2)
            df_pivot["Low"] = round(df_pivot.loc[:, range(0, 7)].sum(axis=1) / total_counts, 2)
            df_pivot["Mod"] = round(df_pivot.loc[:, range(7, 9)].sum(axis=1) / total_counts, 2)
            df_pivot["High"] = round(df_pivot.loc[:, range(9, 11)].sum(axis=1) / total_counts, 2)
            df_pivot["Avg"] = round(
                (df_pivot.loc[:, range(0, 11)] * pd.Series(range(0, 11), index=range(0, 11))).sum(axis=1) / total_counts, 2
            )

            # Ensure all computed columns exist and have float type
            for col in ["Low", "Mod", "High", "Avg"]:
                df_pivot[col] = df_pivot[col].astype(float).fillna(0.0)

            return df_pivot
        except Exception as e:
            print(e)
            return None

    def correlate_data(self):
        """
        Pivot the melted DataFrame to a wide format with respondents as rows and questions as columns,
        then compute and return the correlation matrix of the numeric responses.
        """
        try:
            # Pivot wide: rows are respondents, columns are questions, values are answers
            df_wide = self.df_melt.pivot(index="#", columns="Question", values="Answer")
            # Compute correlation matrix for the questions
            correlation_matrix = df_wide.corr()
            print("correlation matrix computed")
            return correlation_matrix
        except Exception as e:
            print(e)
            return None

def summarize(filename, question, group_filter=None, **filters):
    """
    Process the CSV file with filtering/grouping via PreProcess,
    then extract responses for the specified question and pass them
    to the completion API for summarization.
    """
    # Instantiate PreProcess to apply filters (and group_filter if provided)
    PP = PreProcess(filename, group_filter=group_filter, **filters)
    
    # Extract responses for the given question from the melted (long format) data
    df_question = PP.df_melt[PP.df_melt["Question"] == question]
    if df_question.empty:
        raise ValueError(f"No responses found for question: {question}")
    
    answers_list = df_question["Answer"].dropna().tolist()
    # Combine responses into one string, each on a new line
    answers = "\n".join(str(answer) for answer in answers_list)
    
    # Create the completion request with the extracted responses
    response = client.responses.create(
        model="gpt-4o",
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You are a survey analyst who is gifted at finding meaning and insights "
                            "in long answer responses from respondents. You are given responses to a particular "
                            "question and work through the cause of the feelings of people, how they are related "
                            "and what this should mean to the researchers."
                        )
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            f"The following are responses to the question, '{question}'. "
                            f"The following filters were also applied: {str(filters)}"
                            f"Please summarize and provide insights.\n\n{answers}"
                        )
                    }
                ]
            },
        ],
        temperature=1,
    )
    
    output = response.output[0].content[0].text
    return json.dumps(output)

def get_responses(form_id):
    endpoint = f"/forms/{form_id}/responses?page_size=1000"
    all_responses = []
    url = base_url + endpoint
    headers = {"Authorization": f"Bearer {TYPEFORM_API_KEY}"}

    while url:
        r = requests.get(url, headers=headers)
        data = r.json()
        all_responses.extend(data["items"])
        url = data.get("_links", {}).get("next")  # paginate

    return all_responses

def get_form(form_id):
    endpoint = f"/forms/{form_id}"
    r = requests.get(base_url + endpoint, headers={"Authorization": f"Bearer {TYPEFORM_API_KEY}"})
    return r.json()

def get_typeforms():
    endpoint = "/forms?page_size=200"
    r = requests.get(base_url + endpoint, headers={"Authorization": f"Bearer {TYPEFORM_API_KEY}"})
    return r.json()

def build_csv_from_typeform(form_id):
    form = get_form(form_id)
    questions = {field["id"]: field["title"] for field in form["fields"]}

    responses = get_responses(form_id)

    rows = []
    for r in responses:
        row = {}
        answers = r.get("answers", [])
        row["#"] = r.get("response_id")
        rows.append(row)
        for answer in answers:
            question_id = answer.get("field", {}).get("id")
            question_text = questions.get(question_id, question_id)
            value = answer.get("text") or answer.get("number") or answer.get("choice", {}).get("label")
            row[question_text] = value

    df = pd.DataFrame(rows)
    return json.loads(df.to_json(orient="records"))


