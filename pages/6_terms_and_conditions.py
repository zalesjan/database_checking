import streamlit as st
from modules.database_utils import do_query, show_counts_of_all
from modules.logs import write_and_log

# Set the title and a brief introduction
st.title("DATA ACCESS CONDITIONS")

st.title("Access to the WILDCARD Relational PostgreSQL Database of tree inventory data from the network of permanent research plots in European set-aside forests ")
st.write(
    """
**A. WILDCARD DENDROMETRIC DATABASE ACCESS**

1. Here we specify the conditions under which the Data Curator shall provide the on-line ‘read only’ access to the content of the WILDCARD Relational PostgreSQL dendrometric database to the Data User


And/Or


2. The Data Curator shall carry-out data queries and provide specific data assemblies on the demand of the Data User
)

**B. TERMS and CONDITIONS of DATA USE**
 

1. The data User has rights to use the data only within the framework of the WILDCARD HE project exclusively for purposes specified in the EuFoRIa WILDCARD Project Proposal Parts A and B.


2. In particular, commercial use of the data is not permitted.


3. Any sharing of the data or its directly derived characteristics to third parties (outside the WILDCARD project) is not permitted, with the exception of cases directly linked to publications in scientific journals (see point C4)

Likewise, no rights of access, use or exploitation may be granted or transferred to third parties.

Access to the data is to be regulated by the Data User in such a way that unauthorised data access is excluded.


4. The right of use is limited in time and ends on 31. 12. 2027.

On this date, the data curator must terminate the data access to the Data User. The Data User must delete all provided dendrometric data assemblies stored outside of the central WILDCARD database, with the exception of the data necessary for documentation and archiving purposes and finishing publications directly connected to the WILDCARD project.


5. All rights to the data shall remain with the original data providers.


6. In the event that the data is used contrary to the terms of these conditions, the right to access and use the data is withdrawn with immediate effect.



**C. PUBLICATIONS**


1. If the use of data results in publication, the original data provider is involved in the publication process and updated on a regular basis. It is up to the main author of a publication to decide if the provided data can be integrated in the planned scientific study.


2. Data providers are offered co-authorship provided that they also contribute to the process of manuscript writing and/or revision and editing before submission.


3. The acknowledgments of the resulting publications will include a statement referring to the institution of the original data provider.


4. Before data is delivered for archiving required by a journal in which analyses of the data are published, the nature and form of the data delivered must be agreed upon.




**D. LIABILITY**


1. The data are based on the current state of scientific knowledge. However, there is no liability on the completeness, usefulness or accuracy of the data.  

 
""")

st.markdown('<a href="https://docs.google.com/forms/d/1uVVIM23M4wiwB9gUeDYooiljRZFjuu9pP_BnLVZbGC0/edit" target="_blank">Please fill out this Form to gain access</a>', unsafe_allow_html=True)

# Closing Note
st.header("Contacts")
st.write(
    """
    *If you believe you need insitutional or special access to the database, contact Jan Zálešák (zalesak@vukoz.cz).
    *For any assistance, contact Jan Zálešák (zalesak@vukoz.cz) for support.* 
    *Enjoy using the app!*
    """
)
