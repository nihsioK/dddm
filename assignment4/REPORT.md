# Assignment No4: Scientific and Network Analytics for Innovation Decisions

**Name:** [Student Name]  
**Topic:** Advancing Early-Career Recruitment and Internship Management through Artificial Intelligence and Data Analytics.

---

## Part 1: Data Collection & Preparation

### Search Strategy
- **Query:** `(internship OR "early career" OR "graduate recruitment" OR "entry level jobs")`
- **Keywords:** internship, early career, graduate recruitment, job matching, recommender systems, employability, career development.
- **Time Range:** 2014–2024.
- **Source:** OpenAlex API.

### Data Description
- **N = 1200** records initially fetched.
- **Fields:** `id`, `title`, `year`, `authors`, `journal`, `citations`, `keywords`, `countries`.

### Data Cleaning Process
1. **Deduplication:** Removed 8 duplicate records based on OpenAlex ID and Title.
2. **Normalization:** Author names and Journal titles were normalized to Title Case and stripped of whitespace.
3. **Missing Values:** Handled missing journals and authors by labeling as "Unknown". Empty keywords and countries were filled with empty strings/Unknown.
4. **Filtering:** Kept only works from 2014 onwards.
5. **Final Dataset:** 1192 unique publications.

---

## Part 2: Analysis & Visualization

### Key Visualizations (see `output/figures/`)
- **Dynamics:** Publication volume shows a steady increase, peaking around 2021-2022, likely due to research on remote internships during COVID-19.
- **Top Journals:** Dominated by education and psychology-related journals (e.g., *Journal of Vocational Behavior*, *Medical Education*).
- **Top Countries:** USA, UK, China, and Australia are leaders in this research field.
- **Citations:** Distribution follows a power law, with a few highly influential papers and many with low citation counts.

### Network Analysis
- **Network Type:** Keyword Co-occurrence Network.
- **Centrality:** "Psychology", "Medicine", and "Education" are the most central nodes, indicating that career development is heavily studied through these lenses. "Computer Science" and "Engineering" also show significant centrality.
- **Clusters:** Identified clusters around "Medical Education", "Career Transition", and "STEM Internships".

### Topic Modeling (LDA)
- **Topic 1: Engineering & Career Development** - Focus on career paths in technical fields.
- **Topic 2: Internship & Psychology** - Focus on the student experience and behavioral aspects.
- **Topic 3: Health & Burnout** - Research on the stress of early career transitions.
- **Topic 4: Pedagogy & Economics** - Institutional and economic perspectives on internships.
- **Topic 5: COVID-19 Impact** - Research on how the pandemic changed the early career landscape.

---

## Part 3: Strategic Insights for Staj.kz

### Research Gaps & Fronts
- **Gaps:** Lack of research on internship ecosystems in Central Asia; need for more focus on AI-driven bias reduction in graduate hiring.
- **Fronts:** Automated recruitment systems and the "future of work" post-pandemic.

### Practical Application for Staj.kz
1. **Personalized Matching:** Implement recommendation algorithms based on the "Career Development" research to match students not just by skills, but by long-term career trajectory.
2. **Value-Added Services:** Incorporate psychological well-being and "burnout prevention" tips based on the identified research clusters (Topic 3).
3. **Data-Driven Skill Mapping:** Use the "Pedagogy & Engineering" insights to help students identify and close the skill gap between university and their first job.

---

**Code & Data Links:**
- Data: `assignment4/data/processed/openalex_cleaned.csv`
- Scripts: `assignment4/fetch_data.py`, `assignment4/clean_data.py`, `assignment4/analyze_data.py`
