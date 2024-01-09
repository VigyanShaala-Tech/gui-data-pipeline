# Import libraries
import streamlit as st

def Home():
    # Set page title and icon
    st.set_page_config(
        page_title="VigyanShaala International",
        page_icon="🔬",
    )

    # Define the Power BI link
    power_bi_link = "https://app.powerbi.com/"

    # Create an expander HTML element to navigate to Power BI dashboard
    expander_html = f"""
    <details>
        <summary style="font-size: 18px; font-weight: bold;">Go to Power BI</summary>
        <a href="{power_bi_link}" target="_blank">
            <button style="font-size: 18px; padding: 10px 20px; background-color: #4CAF50; color: white; border: none; cursor: pointer; width: 100%;">Open Power BI Dashboard</button>
        </a>
    </details>
    """

    # Display the expander HTML element in the sidebar
    st.sidebar.markdown(expander_html, unsafe_allow_html=True)
    st.sidebar.info("**GUI**: Kalpana Program GUI")

    # Display the VigyanShaala logo in the sidebar
    st.sidebar.image("assets/VS-logo.png")

    # Display the title of the web app
    st.markdown(
        f"<h1 style='text-align: center; color: Black; font-size: 50px;'>VigyanShaala International</h1>",
        unsafe_allow_html=True,
    )

    # Create columns for layout
    col1, col2, col3 = st.columns([1, 6, 1])

    # Add spacing in the first and third columns
    with col1:
        st.write("")

    with col3:
        st.write("")

    # Display the VigyanShaala logo in the second column
    with col2:
        st.image("assets/VS-logo.png")

    # Display a description of VigyanShaala's mission
    st.markdown(
        '<p style="font-size:22px; text-align: center; color: black;font-size: 25px;">Enabling innovators of tomorrow to achieve their dreams by bringing science, technology, and learning to their doorsteps</p>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # Display the "About Us" section
    st.markdown(
        f"<h2 style='text-align: center; color: yellow; background-color: black;'>About Us</h2>",
        unsafe_allow_html=True,
    )
    st.write("")

    st.markdown(
        f"<p style='text-align: center; color: black; font-size: 20px'>💠 'VigyanShaala (विज्ञानशाला)'​, in Sanskrit, means 'A Creative Classroom of Science'. VigyanShaala is on a Mission to make Quality STEM (Science, Technology, Engineering and Mathematics) education and skills accessible to the most marginalized. </p>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Display the "Specialties" section
    st.markdown(
        f"<h2 style='text-align: center; color: yellow; background-color: black;'>Specialties</h2>",
        unsafe_allow_html=True,
    )
    st.write("")

    st.markdown(
        f"<p style='text-align: center; color: black; font-size: 20px'>💠STEM Skills, makerspace, project-based learning, hands-on learning, Teacher Training, Youth Engagement, UNSDGs, Volunteer engagement, Women In STEM, Education Policy, Consultancy, and Collaboration</p>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Display the "Contact Information" section
    st.markdown(
        f"<h2 style='text-align: center; color: yellow; background-color: black;'>Contact Information</h2>",
        unsafe_allow_html=True,
    )
    st.write("")

    st.markdown(
        f"<p style='text-align: center; color: black; font-size: 20px'>Website: <a href='https://mytribe.vigyanshaala.com/' target='_blank'>VigyanShaala</a></p>",
        unsafe_allow_html=True,
    )

    st.markdown(
        f"<p style='text-align: center; color: black; font-size: 20px'>Phone: <a href='tel:+919322858684'>+919322858684</a></p>",
        unsafe_allow_html=True,
    )

    st.markdown(
        f"<p style='text-align: center; color: black; font-size: 20px'>LinkedIn: <a href='https://www.linkedin.com/company/vigyanshaala' target='_blank'>VigyanShaala</a></p>",
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    Home()

