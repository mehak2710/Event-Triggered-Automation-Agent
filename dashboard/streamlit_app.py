import os

import httpx
import pandas as pd
import streamlit as st

API = os.getenv("API_URL", "http://localhost:8000")
HEADERS = {"X-Admin-Token": os.getenv("ADMIN_TOKEN", "")}

st.set_page_config(page_title="Event Automation Agent", layout="wide")
st.title("Event Automation Agent")
st.caption("Live view of the event stream, retries and dead letter queue")


def api_get(path: str, **params):
    resp = httpx.get(f"{API}{path}", headers=HEADERS, params=params, timeout=5)
    resp.raise_for_status()
    return resp.json()


def api_post(path: str):
    resp = httpx.post(f"{API}{path}", headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


@st.fragment(run_every=3)
def live():
    try:
        stats = api_get("/admin/stats")
        events = api_get("/admin/events", limit=50)
        dead = api_get("/admin/dlq")
    except Exception as exc:
        st.error(f"API unreachable: {exc}")
        return

    m = stats["metrics"]
    cols = st.columns(6)
    cols[0].metric("Received", m.get("received", 0))
    cols[1].metric("Processed", m.get("processed", 0))
    cols[2].metric("Retries", m.get("retries", 0))
    dupes = int(m.get("duplicates", 0)) + int(m.get("edge_duplicates", 0))
    cols[3].metric("Duplicates blocked", dupes)
    cols[4].metric("Pending", stats["pending"])
    cols[5].metric("Dead-lettered", stats["dlq_length"])

    st.subheader("Recent events")
    if events:
        st.dataframe(pd.DataFrame(events), hide_index=True)
    else:
        st.info("No events processed yet.")

    st.subheader("Dead letter queue")
    if not dead:
        st.success("Dead letter queue is empty.")
        return
    if st.button("Replay all"):
        api_post("/admin/dlq/replay-all")
        st.rerun(scope="fragment")
    for row in dead:
        title = f"{row['event_id']} ({row['type']}) - {row['reason']}"
        with st.expander(title):
            st.write(f"Attempts: {row['attempts']}")
            st.code(row["error"])
            st.json(row["payload"])
            if st.button("Replay", key=row["entry_id"]):
                api_post(f"/admin/dlq/{row['entry_id']}/replay")
                st.rerun(scope="fragment")


live()