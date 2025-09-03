"""
Main application file for the People Counting Analysis dashboard.

This Streamlit application provides a user interface to visualize and analyze
people counting data from various stores. It includes user authentication,
data filtering, and interactive charts.
"""
import calendar
import numpy as np
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml

from datetime import date
from plotly import graph_objs as go
from yaml.loader import SafeLoader

# Import database functions and the engine object.
from database import engine, dbStore, dbNumCrowd, dbErrLog

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="People Counting System", 
    page_icon="📈", 
    layout="wide"
)

# --- DATABASE CONNECTION CHECK ---
# Stop the application if the database connection failed on startup.
if engine is None:
    st.warning("Ứng dụng không thể khởi động do không có kết nối CSDL. Vui lòng xem lại thông báo lỗi ở trên.")
    st.stop()

# --- STYLING ---
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# --- AUTHENTICATION ---
if "authentication_status" not in st.session_state:
    st.session_state.authentication_status = ""

with open("config.yaml", encoding="utf-8") as file:
    config = yaml.load(file, Loader=SafeLoader)
    
    authenticator = stauth.Authenticate(
        config["credentials"],
        config["cookie"]["name"],
        config["cookie"]["key"],
        config["cookie"]["expiry_days"]
    )
    authenticator.login()

    if st.session_state.authentication_status is False:
        st.error("Tên đăng nhập hoặc mật khẩu không chính xác")
    elif st.session_state.authentication_status is None:
        st.warning("Vui lòng nhập tên đăng nhập và mật khẩu")

# --- MAIN APPLICATION LOGIC ---
# Only display the main app if the user is successfully authenticated.
if st.session_state.authentication_status:
    def getWeekNums(year: str) -> pd.DataFrame:
        """
        Generates a DataFrame of week numbers and their corresponding date ranges for a given year.

        Args:
            year (str): The year for which to generate the week numbers.

        Returns:
            pd.DataFrame: A DataFrame with formatted week strings for display.
        """
        start_date, end_date = f"1/1/{year}", f"12/31/{year}"
        data = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq="D"), columns=["date"])
        data["year_calendar"] = data["date"].dt.isocalendar().year
        data["week_calendar"] = data["date"].dt.isocalendar().week

        group = data.groupby(["year_calendar", "week_calendar"]).agg({"date": ["min", "max"]}).reset_index()
        group["week_num"] = np.where(group["week_calendar"][0] == 52, group["week_calendar"] + 1, group["week_calendar"])
        if group["week_num"][0] == 53: group.at[0, "week_num"] = 1

        group["week"] = "WK" + group["week_num"].astype(str) + \
                        " (" + group["date"]["min"].dt.strftime("%d/%m/%y") + \
                        " - " + group["date"]["max"].dt.strftime("%d/%m/%y") + ")"
        return group

    @st.cache_data(ttl=900, show_spinner=False)
    def filter(data, store=0, date=None, year=None, week=None, month=None, quarter=None) -> pd.DataFrame:
        """Filters the raw people counting data based on user-selected criteria."""
        if date:
            data = data[data.recordtime.dt.date == date]
        else:
            data = data[data.recordtime.dt.year == year]
            if week:
                data = data[data.recordtime.dt.strftime("%W").astype(int) == week]
            elif month:
                data = data[data.recordtime.dt.strftime("%B") == month]
            elif quarter:
                data = data[data.recordtime.dt.to_period("Q").dt.strftime("%q").astype(int) == quarter + 1]

        if store > 0:
            data = data[data.storeid == store]
        return data.sort_values(by="recordtime").reset_index(drop=True)

    @st.cache_data(ttl=900, show_spinner=False)
    def clean(data: pd.DataFrame, option: str, period: int = None) -> pd.DataFrame:
        """
        Cleans and transforms the filtered data for analysis and visualization.

        Args:
            data (pd.DataFrame): The filtered input data.
            option (str): The selected reporting period ('Daily', 'Weekly', etc.).
            period (int): The sub-period for daily reports (e.g., 15min, 30min).

        Returns:
            pd.DataFrame: A processed DataFrame ready for display.
        """
        data.drop(["out_num", "position", "storeid"], axis=1, inplace=True)
        data = data.set_index('recordtime').between_time("6:30:00", "23:59:59").reset_index()

        # Handle sensor errors: cap abnormally high 'in_num' values at 1.
        # This prevents outliers from skewing the analysis.
        data["in_num"] = data.in_num.where(data.in_num < 100, 1).apply(np.int64)

        # Resample data based on the chosen time frame.
        if option == "Daily":
            freqs = ["15min", "30min", "H"]
            data = data.resample(freqs[period], on="recordtime").sum()
            data.index = data.index.strftime("%H:%M")
        elif option == "Weekly":
            data = data.resample('D', on="recordtime").sum()
            data["Day"] = data.index.day_name()
            data = data[["Day", "in_num"]]
            data.index = data.index.strftime("%d/%m/%Y")
        elif option == "Monthly":
            data = data.resample("D", on="recordtime").sum()
            data.index = data.index.strftime("%d/%m/%Y")
        else: # Yearly or Quarter
            data = data.resample("M", on="recordtime").sum()
            data.index = data.index.strftime("%m/%Y")

        # Calculate analytical columns.
        data["Percentage"] = (data.in_num / data.in_num.sum()).map("{:.2%}".format)
        data["Relative Ratio"] = data.in_num.pct_change().map("{:.2%}".format, na_action="ignore")

        data.rename(columns={"in_num": "Quantity"}, inplace=True)
        data.index.names = ["Time"]

        return data

    # --- SIDEBAR FILTERS ---
    stores = dbStore()
    date_, period_, year_, week_, month_, quarter_ = None, None, None, None, None, None

    with st.sidebar:
        st.image("logo.png")

        with st.expander(f"Welcome *{st.session_state.username.title()}*"):
            authenticator.logout("Logout", "main")

        display_stores = tuple(["All"] + stores["name"].to_list())
        store_ = st.selectbox("Cửa hàng:", display_stores)
        option_ = st.selectbox("Thống kê theo:", ("Daily", "Weekly", "Monthly", "Quarter", "Yearly"), index=2)

        # Dynamically show date/time filters based on the main selection.
        if option_ == "Daily":
            date_ = st.date_input("Ngày:", date.today())
            display_periods = ("Mỗi 15 phút", "Mỗi 30 phút", "Mỗi giờ")
            period_ = st.radio("Chu kỳ:", range(len(display_periods)), format_func=lambda x: display_periods[x], index=2)
        else:
            year_ = st.selectbox("Năm:", reversed(range(2018, date.today().year + 1)))

            if option_ == "Weekly":
                weeks = getWeekNums(str(year_))
                week_ = st.selectbox("Tuần:", range(len(weeks["week"])), format_func=lambda x: weeks["week"][x], index=int(date.today().strftime("%W")))
            elif option_ == "Monthly":
                month_ = st.selectbox("Tháng:", calendar.month_name[1:], index=date.today().month - 1)
            elif option_ == "Quarter":
                display_quarters = ("Quý 1 (Xuân)", "Quý 2 (Hạ)", "Quý 3 (Thu)", "Quý 4 (Đông)")
                quarter_ = st.selectbox("Quý:", range(len(display_quarters)), format_func=lambda x: display_quarters[x], index=(date.today().month - 1) // 3)

    # --- PAGE CONTENT ---
    with st.container():
        st.title("Hệ thống Phân tích Lượt người ra vào")

        # Admin-only section for viewing error logs.
        if st.session_state.username == "admin":
            with st.expander("**_NHẬT KÝ LỖI HỆ THỐNG (ERROR LOG)_**", expanded=False):
                err_log = dbErrLog()
                if not err_log.empty:
                    err_log = err_log.groupby(["storeid", "ErrorMessage"]).max().reset_index()
                    err_log = err_log.drop(columns=["ID", "Errorcode", "DeviceCode"], axis=1)
                    err_log = err_log.merge(stores[["tid", "name"]].rename(columns={"tid": "storeid"}), on="storeid", how="left").set_index("LogTime")
                    err_log.drop("storeid", axis=1, inplace=True)
                    err_log.insert(0, "Name", err_log.pop("name"))
                    err_log.sort_index(ascending=False, inplace=True)
                    err_log.Name = err_log.Name.replace(r"\s+", " ", regex=True)
                    err_log.ErrorMessage = err_log.ErrorMessage.replace(r"\s+", " ", regex=True)

                    st.dataframe(err_log, width='stretch')
                else:
                    st.info("Không có lỗi nào được ghi nhận.")

        with st.expander("**_BÁO CÁO THỐNG KÊ_**", expanded=True):
            # Fetch data for the selected year to optimize performance.
            selected_year = year_ if year_ else date_.year
            num_crowd = dbNumCrowd(selected_year)

            if not num_crowd.empty:
                store_id = 0 if store_ == "All" else stores[stores["name"] == store_]["tid"].iloc[0]

                # Filter and process data based on sidebar selections.
                data = filter(num_crowd, store_id, date_, year_, week_, month_, quarter_)
                if not data.empty:
                    data = clean(data, option_, period_)

                    # Create and display the bar chart.
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=data.index, y=data.Quantity, name="Quantity", showlegend=False))
                    fig.update_layout(go.Layout(
                        autosize=True, 
                        height=300, 
                        margin=go.layout.Margin(l=10, r=10, b=5, t=30, pad=0)
                    ))
                    st.plotly_chart(fig, width='stretch')

                    # Display the processed data table.
                    st.dataframe(data, width='stretch')
                else:
                    st.warning("Không có dữ liệu cho lựa chọn này.")
            else:
                st.warning(f"Không có dữ liệu cho năm {selected_year}.")
