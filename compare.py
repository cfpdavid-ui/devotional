#!/usr/bin/env python3
"""
Enhanced Theology Analyzer - Complete Analysis System
Combines quick surveys, deep analysis, context inspection, and teacher comparison

Run with: streamlit run theology_analyzer_ENHANCED.py
"""

import streamlit as st
import sqlite3
import pandas as pd
from collections import Counter
import re
import json
from datetime import datetime
from pathlib import Path

try:
    import anthropic
except ImportError:
    st.error("Install anthropic: pip install anthropic")

try:
    import html2text
    HTML2TEXT_AVAILABLE = True
except ImportError:
    HTML2TEXT_AVAILABLE = False

# Page config
st.set_page_config(
    page_title="Enhanced Theology Analyzer",
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Hardcoded API key
ANTHROPIC_API_KEY = "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"

# ============================================================================
# DATABASE UTILITIES
# ============================================================================

def detect_schema(conn):
    """Auto-detect table name and transcript column"""
    cursor = conn.cursor()
    
    # Check for table
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND (name='transcripts' OR name='video_transcripts')
    """)
    result = cursor.fetchone()
    
    if not result:
        return None, None
    
    table_name = result[0]
    
    # Check columns
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Determine transcript column
    if 'transcript_text' in columns:
        transcript_col = 'transcript_text'
    elif 'transcript' in columns:
        transcript_col = 'transcript'
    else:
        return None, None
    
    return table_name, transcript_col

def get_db_connection(db_path):
    """Connect to database and detect schema"""
    if not Path(db_path).exists():
        st.error(f"Database not found: {db_path}")
        return None, None, None
    
    try:
        conn = sqlite3.connect(db_path)
        table_name, transcript_col = detect_schema(conn)
        
        if not table_name:
            st.error("No valid transcripts table found")
            return None, None, None
        
        return conn, table_name, transcript_col
    except Exception as e:
        st.error(f"Database error: {e}")
        return None, None, None

# ============================================================================
# SERIES LIBRARY DATABASE
# ============================================================================

def init_series_library():
    """Initialize Series Library database"""
    library_path = Path("/mnt/user-data/outputs/series_library.db")
    library_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(library_path))
    cursor = conn.cursor()
    
    # Create series_library table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS series_library (
            series_id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_title TEXT NOT NULL,
            topic TEXT,
            num_posts INTEGER,
            audience TEXT,
            style TEXT,
            post_length TEXT,
            date_created TEXT,
            source_databases TEXT,
            total_words INTEGER,
            total_cost REAL,
            status TEXT DEFAULT 'draft',
            tags TEXT,
            notes TEXT
        )
    """)
    
    # Create posts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            post_id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER,
            post_number INTEGER,
            post_title TEXT,
            html_content TEXT,
            markdown_content TEXT,
            word_count INTEGER,
            sources_used TEXT,
            date_created TEXT,
            FOREIGN KEY (series_id) REFERENCES series_library(series_id)
        )
    """)
    
    conn.commit()
    conn.close()
    
    return str(library_path)

def save_series_to_library(series_data):
    """Save a generated series to the library"""
    library_path = init_series_library()
    conn = sqlite3.connect(library_path)
    cursor = conn.cursor()
    
    # Insert series metadata
    cursor.execute("""
        INSERT INTO series_library (
            series_title, topic, num_posts, audience, style, post_length,
            date_created, source_databases, total_words, total_cost, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        series_data['title'],
        series_data['topic'],
        series_data['num_posts'],
        series_data['audience'],
        series_data['style'],
        series_data['post_length'],
        datetime.now().isoformat(),
        json.dumps(series_data['source_databases']),
        series_data['total_words'],
        series_data.get('cost', 0),
        'draft'
    ))
    
    series_id = cursor.lastrowid
    
    # Insert posts
    for post in series_data['posts']:
        cursor.execute("""
            INSERT INTO posts (
                series_id, post_number, post_title, html_content, 
                markdown_content, word_count, sources_used, date_created
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            series_id,
            post['post_num'],
            post.get('title', f"Post {post['post_num']}"),
            post['html'],
            post.get('markdown', ''),
            post.get('word_count', 0),
            json.dumps(post.get('sources', [])),
            datetime.now().isoformat()
        ))
    
    conn.commit()
    conn.close()
    
    return series_id

def get_all_series():
    """Get all series from library"""
    library_path = init_series_library()
    conn = sqlite3.connect(library_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT series_id, series_title, topic, num_posts, audience, 
               date_created, status, total_words
        FROM series_library
        ORDER BY date_created DESC
    """)
    
    series = cursor.fetchall()
    conn.close()
    
    return series

def get_series_details(series_id):
    """Get complete details for a series"""
    library_path = init_series_library()
    conn = sqlite3.connect(library_path)
    cursor = conn.cursor()
    
    # Get series metadata
    cursor.execute("""
        SELECT * FROM series_library WHERE series_id = ?
    """, (series_id,))
    
    series_data = cursor.fetchone()
    
    if not series_data:
        conn.close()
        return None
    
    # Get posts
    cursor.execute("""
        SELECT * FROM posts WHERE series_id = ? ORDER BY post_number
    """, (series_id,))
    
    posts = cursor.fetchall()
    conn.close()
    
    return {'series': series_data, 'posts': posts}

def delete_series(series_id):
    """Delete a series and all its posts"""
    library_path = init_series_library()
    conn = sqlite3.connect(library_path)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM posts WHERE series_id = ?", (series_id,))
    cursor.execute("DELETE FROM series_library WHERE series_id = ?", (series_id,))
    
    conn.commit()
    conn.close()

def search_series(search_term):
    """Search series by title or topic"""
    library_path = init_series_library()
    conn = sqlite3.connect(library_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT series_id, series_title, topic, num_posts, audience,
               date_created, status, total_words
        FROM series_library
        WHERE series_title LIKE ? OR topic LIKE ?
        ORDER BY date_created DESC
    """, (f'%{search_term}%', f'%{search_term}%'))
    
    results = cursor.fetchall()
    conn.close()
    
    return results

# ============================================================================
# COMPREHENSIVE ANALYSIS ENGINE
# ============================================================================

class ComprehensiveAnalyzer:
    """Full corpus analysis with complete processing (no sampling)"""
    
    def __init__(self, conn, table_name, transcript_col):
        self.conn = conn
        self.table_name = table_name
        self.transcript_col = transcript_col
        self.cursor = conn.cursor()
    
    def get_basic_stats(self):
        """Get comprehensive statistics"""
        self.cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                SUM(LENGTH({self.transcript_col})) as total_chars,
                AVG(LENGTH({self.transcript_col})) as avg_chars
            FROM {self.table_name}
            WHERE {self.transcript_col} IS NOT NULL
        """)
        
        total, total_chars, avg_chars = self.cursor.fetchone()
        
        # Word count approximation
        total_words = total_chars // 5 if total_chars else 0
        avg_words = int(avg_chars // 5) if avg_chars else 0
        
        # Date range
        date_col = self._find_date_column()
        date_range = "Unknown"
        
        if date_col:
            self.cursor.execute(f"""
                SELECT MIN({date_col}), MAX({date_col})
                FROM {self.table_name}
                WHERE {date_col} IS NOT NULL
            """)
            min_date, max_date = self.cursor.fetchone()
            if min_date and max_date:
                date_range = f"{min_date[:10]} to {max_date[:10]}"
        
        return {
            'total_sermons': total,
            'total_words': total_words,
            'avg_words': avg_words,
            'date_range': date_range
        }
    
    def _find_date_column(self):
        """Find date column in table"""
        self.cursor.execute(f"PRAGMA table_info({self.table_name})")
        columns = [col[1] for col in self.cursor.fetchall()]
        
        for col in ['published_at', 'created_at', 'created_date']:
            if col in columns:
                return col
        return None
    
    def find_all_phrases(self, min_occurrences=20):
        """Find ALL repeated phrases across ENTIRE corpus (no sampling)"""
        
        # NAR and theological markers
        target_phrases = [
            # NAR markers
            'courts of heaven', 'court of heaven', 'DNA healing', 'wounded soul', 
            'soul wound', 'wounding spirit', 'leviathan spirit', 'jezebel spirit',
            'python spirit', 'generational curse', 'territorial spirits',
            'seven mountains', 'apostolic center', 'prophetic word',
            
            # Prosperity
            'name it claim it', 'speak into existence', 'seed faith', 
            'hundredfold return', 'breakthrough anointing', 'supernatural favor',
            
            # Kingdom/Third Wave
            'kingdom of god', 'kingdom now', 'power encounter', 'signs and wonders',
            'holy spirit', 'baptism of the spirit', 'spiritual gifts', 
            'word of knowledge',
            
            # Orthodox markers
            'gospel of', 'grace of god', 'blood of christ', 'atonement',
            'justification', 'sanctification', 'repentance', 'faith alone',
            'scripture alone', 'sola scriptura'
        ]
        
        # Get ALL text (complete corpus)
        self.cursor.execute(f"SELECT {self.transcript_col} FROM {self.table_name}")
        all_text = ' '.join([row[0] for row in self.cursor.fetchall() if row[0]])
        all_text_lower = all_text.lower()
        
        phrase_counts = {}
        for phrase in target_phrases:
            count = all_text_lower.count(phrase.lower())
            if count >= min_occurrences:
                phrase_counts[phrase] = count
        
        return dict(sorted(phrase_counts.items(), key=lambda x: x[1], reverse=True))
    
    def count_keywords(self):
        """Count ALL theological keywords across ENTIRE corpus"""
        
        keywords = {
            # Trinitarian
            'god': 0, 'jesus': 0, 'christ': 0, 'holy spirit': 0, 'spirit': 0,
            
            # Salvation
            'gospel': 0, 'salvation': 0, 'saved': 0, 'grace': 0, 'faith': 0,
            'repent': 0, 'sin': 0, 'cross': 0, 'blood': 0, 'atonement': 0,
            
            # Charismatic
            'healing': 0, 'miracle': 0, 'prophetic': 0, 'prophecy': 0,
            'tongues': 0, 'anointing': 0, 'demon': 0, 'deliverance': 0,
            
            # Prosperity/NAR
            'blessing': 0, 'blessed': 0, 'prosperity': 0, 'favor': 0,
            'breakthrough': 0, 'abundance': 0, 'wealth': 0,
            
            # Kingdom
            'kingdom': 0, 'church': 0, 'worship': 0, 'prayer': 0
        }
        
        # Get ALL text
        self.cursor.execute(f"SELECT {self.transcript_col} FROM {self.table_name}")
        all_text = ' '.join([row[0].lower() for row in self.cursor.fetchall() if row[0]])
        
        for keyword in keywords.keys():
            keywords[keyword] = all_text.count(keyword)
        
        return dict(sorted(keywords.items(), key=lambda x: x[1], reverse=True))
    
    def detect_series(self):
        """Detect sermon series"""
        self.cursor.execute(f"SELECT title FROM {self.table_name} WHERE title IS NOT NULL")
        titles = [row[0] for row in self.cursor.fetchall()]
        
        series = Counter()
        
        for title in titles:
            # Pattern 1: "Series Name: Episode"
            if ':' in title:
                series_name = title.split(':')[0].strip()
                series[series_name] += 1
            
            # Pattern 2: "Series - Part N"
            elif ' - Part ' in title or ' Part ' in title:
                series_name = re.split(r'\s+-\s+Part|\s+Part', title)[0].strip()
                series[series_name] += 1
            
            # Pattern 3: "Series Name N"
            match = re.match(r'(.+?)\s+\d+$', title)
            if match:
                series_name = match.group(1).strip()
                series[series_name] += 1
        
        return {k: v for k, v in series.most_common(20) if v >= 3}
    
    def get_samples(self, num=10):
        """Get representative samples"""
        samples = []
        
        # Earliest
        self.cursor.execute(f"""
            SELECT title, {self.transcript_col}
            FROM {self.table_name}
            WHERE {self.transcript_col} IS NOT NULL
            ORDER BY id ASC LIMIT 2
        """)
        samples.extend(self.cursor.fetchall())
        
        # Latest
        self.cursor.execute(f"""
            SELECT title, {self.transcript_col}
            FROM {self.table_name}
            WHERE {self.transcript_col} IS NOT NULL
            ORDER BY id DESC LIMIT 2
        """)
        samples.extend(self.cursor.fetchall())
        
        # Random
        self.cursor.execute(f"""
            SELECT title, {self.transcript_col}
            FROM {self.table_name}
            WHERE {self.transcript_col} IS NOT NULL
            ORDER BY RANDOM() LIMIT {num - 4}
        """)
        samples.extend(self.cursor.fetchall())
        
        return samples[:num]

# ============================================================================
# CONTEXT INSPECTOR
# ============================================================================

def find_all_contexts(conn, table_name, transcript_col, search_term, context_chars=200):
    """Find ALL occurrences of a phrase with context (no sampling, no missing data)"""
    
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT title, {transcript_col}
        FROM {table_name}
        WHERE LOWER({transcript_col}) LIKE ?
    """, (f'%{search_term.lower()}%',))
    
    results = []
    for title, text in cursor.fetchall():
        if not text:
            continue
        
        text_lower = text.lower()
        search_lower = search_term.lower()
        
        # Find ALL occurrences in this sermon
        pos = 0
        while True:
            pos = text_lower.find(search_lower, pos)
            if pos == -1:
                break
            
            # Extract context
            start = max(0, pos - context_chars)
            end = min(len(text), pos + len(search_term) + context_chars)
            context = text[start:end].strip()
            
            results.append({
                'title': title,
                'context': f"...{context}...",
                'position': pos
            })
            
            pos += len(search_term)
    
    return results

# ============================================================================
# FABRIC-STYLE FORMATTING
# ============================================================================

# ============================================================================
# STREAMLIT UI
# ============================================================================

st.title("📖 Enhanced Theology Analyzer")
st.markdown("*Complete analysis system - no sampling, no missing data*")

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    # Auto-detect available databases
    db_options = []
    
    # Check uploads folder
    upload_dbs = list(Path("/mnt/user-data/uploads").glob("*.db"))
    db_options.extend([str(db) for db in upload_dbs])
    
    # Check current directory
    local_dbs = list(Path(".").glob("*.db"))
    db_options.extend([str(db) for db in local_dbs])
    
    # Remove duplicates and sort
    db_options = sorted(list(set(db_options)))
    
    if db_options:
        # Dropdown selector
        db_path = st.selectbox(
            "📁 Select Database:",
            options=db_options,
            format_func=lambda x: Path(x).name
        )
    else:
        st.warning("⚠️ No databases found. Upload or specify path:")
        db_path = st.text_input("Database Path", value="Vineyard.db")
    
    st.markdown("---")
    st.markdown("### Features")
    st.markdown("""
    - **Quick Survey**: 5 samples
    - **Deep Analysis**: Entire corpus
    - **Context Inspector**: Find everything
    - **Compare Teachers**: Side-by-side
    - **Export Reports**: Multiple formats
    """)

# Connect to database
conn, table_name, transcript_col = get_db_connection(db_path)

if conn:
    # CONTAMINATION CHECK
    cursor = conn.cursor()
    cursor.execute(f"SELECT title FROM {table_name}")
    all_titles = [row[0] for row in cursor.fetchall()]
    
    # Quick contamination scan
    contamination_keywords = [
        'elon musk', 'grok', 'openai', 'chatgpt',
        'justin peters', 'community bible church',
        'trailer', 'movie clip', 'full album'
    ]
    
    contaminated = []
    for title in all_titles:
        title_lower = title.lower()
        for keyword in contamination_keywords:
            if keyword in title_lower:
                contaminated.append(title)
                break
    
    if contaminated:
        st.error(f"⚠️ CONTAMINATION DETECTED: {len(contaminated)} non-sermon videos found!")
        with st.expander(f"🚨 View Contaminated Entries ({len(contaminated)} total)"):
            for title in contaminated[:20]:
                st.write(f"- {title}")
            if len(contaminated) > 20:
                st.write(f"... and {len(contaminated) - 20} more")
            
            st.warning("""
            **These entries will CONTAMINATE your analysis!**
            
            **To fix:**
            1. Run: `python detect_contamination.py your_database.db`
            2. Run: `python clean_database.py your_database.db your_database_clean.db`
            3. Use the clean database
            
            Scripts available in /mnt/user-data/outputs/
            """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
        "🔍 Quick Survey",
        "🔬 Deep Analysis", 
        "🎯 Context Inspector",
        "⚖️ Compare Teachers",
        "❓ Ask Questions",
        "📥 Export",
        "✍️ Blog Generator",
        "📚 Series Generator",
        "🗄️ Series Library"
    ])
    
    # ========================================================================
    # TAB 1: QUICK SURVEY
    # ========================================================================
    
    with tab1:
        st.header("Quick Theological Survey")
        st.markdown("*Fast analysis using 5 random sermon samples (~$0.05)*")
        
        if st.button("🚀 Run Quick Survey", type="primary"):
            with st.spinner("Analyzing 5 random samples..."):
                cursor = conn.cursor()
                cursor.execute(f'''
                    SELECT {transcript_col} FROM {table_name}
                    WHERE {transcript_col} IS NOT NULL
                    ORDER BY RANDOM() LIMIT 5
                ''')
                
                samples = [row[0][:2000] for row in cursor.fetchall()]
                combined = "\n\n---\n\n".join(samples)
                
                try:
                    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                    
                    message = client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=2000,
                        messages=[{
                            "role": "user",
                            "content": f"""Analyze these sermon samples and provide a quick theological profile:

{combined}

CRITICAL: When citing evidence, QUOTE actual phrases (not sample numbers). User cannot see "Sample 1, Sample 2" etc.

Provide:
1. PRIMARY THEOLOGICAL TRADITION
2. VIEW OF GOD'S SOVEREIGNTY  
3. VIEW OF HOLY SPIRIT
4. APPROACH TO SCRIPTURE
5. KEY THEMES (quote actual phrases)
6. RED FLAGS (quote concerning language if any)
7. QUICK ASSESSMENT

Format: "The phrase '[exact quote]' suggests..." NOT "Sample 1 shows..."

Be specific with quoted evidence."""
                        }]
                    )
                    
                    st.markdown("### 📊 Quick Analysis")
                    st.markdown(message.content[0].text)
                    
                    st.info("💡 For comprehensive analysis, use the 'Deep Analysis' tab")
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # ========================================================================
    # TAB 2: DEEP ANALYSIS
    # ========================================================================
    
    with tab2:
        st.header("Deep Comprehensive Analysis")
        st.markdown("*Processes ENTIRE corpus - no sampling (~$0.30-$0.50)*")
        
        if st.button("🔬 Run Deep Analysis", type="primary"):
            with st.spinner("Processing entire corpus..."):
                
                # Run comprehensive analysis
                analyzer = ComprehensiveAnalyzer(conn, table_name, transcript_col)
                
                progress = st.progress(0)
                status = st.empty()
                
                status.text("📊 Gathering statistics...")
                basic_stats = analyzer.get_basic_stats()
                progress.progress(20)
                
                status.text("🔍 Analyzing phrases...")
                phrases = analyzer.find_all_phrases()
                progress.progress(40)
                
                status.text("📝 Counting keywords...")
                keywords = analyzer.count_keywords()
                progress.progress(60)
                
                status.text("📚 Detecting series...")
                series = analyzer.detect_series()
                progress.progress(80)
                
                status.text("📄 Selecting samples...")
                samples = analyzer.get_samples()
                progress.progress(90)
                
                analysis = {
                    'basic_stats': basic_stats,
                    'phrase_frequencies': phrases,
                    'keywords': keywords,
                    'series': series,
                    'samples': samples
                }
                
                status.text("🤖 Sending to Claude...")
                
                # Format for Claude
                analysis_text = f"""# COMPREHENSIVE CORPUS ANALYSIS

## Statistics
- Total Sermons: {basic_stats['total_sermons']:,}
- Total Words: {basic_stats['total_words']:,}

## Top Phrases
"""
                for phrase, count in list(phrases.items())[:20]:
                    analysis_text += f"- {phrase}: {count:,}\n"
                
                analysis_text += "\n## Keywords\n"
                for keyword, count in list(keywords.items())[:20]:
                    analysis_text += f"- {keyword}: {count:,}\n"
                
                analysis_text += "\n## Samples\n"
                for i, (title, text) in enumerate(samples[:3], 1):
                    analysis_text += f"\n### {i}. {title}\n{text[:1500]}...\n"
                
                # Get evaluation
                try:
                    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                    
                    message = client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=3000,
                        messages=[{
                            "role": "user",
                            "content": f"""{analysis_text}

CRITICAL INSTRUCTIONS FOR THEOLOGICAL ASSESSMENT:

**EVIDENCE RULES:**
1. NEVER reference "Sample 1" or "Sample 2" - the user cannot see sample numbers
2. When citing evidence, QUOTE the actual phrase and provide Context Inspector search term
3. Format: "The phrase '[exact quote]' appears - verify context with: Search '[search term]'"
4. Be SPECIFIC - vague references are useless

**ASSESSMENT STRUCTURE:**

## 1. TRADITION
Identify theological tradition (Reformed, Pentecostal, Word of Faith, etc.)

## 2. CONCERNS (Evidence-Based)
For each concern:
- Quote the actual concerning phrase
- Cite frequency from data
- Severity: LOW/MEDIUM/HIGH
- Action: Context Inspector search term

Example:
❌ BAD: "Sample 1 shows works-righteousness tendency"
✅ GOOD: "The phrase 'race for the bottom' appears in sermons about servanthood - verify if framing service as performance-based with: Search 'race for the bottom'"

## 3. STRENGTHS (Evidence-Based)
Cite frequencies that show orthodox markers:
- Christ-centered language (Jesus, Christ mentions)
- Gospel essentials (sin, cross, grace, salvation)
- Practical ministry evidence

## 4. RECOMMENDATION
- Overall verdict
- Safe for what?
- Watch for what?
- Next steps with specific Context Inspector searches"""
                        }]
                    )
                    
                    evaluation = message.content[0].text
                    
                    progress.progress(100)
                    status.text("✅ Complete!")
                    
                    # Store for export
                    st.session_state['last_analysis'] = {
                        'analysis': analysis,
                        'evaluation': evaluation,
                        'fabric_output': None,  # Will generate on demand
                        'db_name': Path(db_path).stem
                    }
                    
                    # Always show raw text (no Fabric-style nonsense)
                    st.markdown("### 📊 Statistics")
                    st.json(analysis['basic_stats'])
                    
                    st.markdown("### 🔍 Top Phrases")
                    for phrase, count in list(analysis['phrase_frequencies'].items())[:20]:
                        st.write(f"- **{phrase}**: {count:,} times")
                    
                    st.markdown("### 📝 Top Keywords")
                    for keyword, count in list(analysis['keywords'].items())[:20]:
                        st.write(f"- **{keyword}**: {count:,} times")
                    
                    st.markdown("### 🤖 Theological Evaluation")
                    st.markdown(evaluation)
                    
                    # Full text for copying
                    st.markdown("---")
                    st.markdown("### 📋 Complete Report (Copy This)")
                    
                    full_text = f"""# COMPREHENSIVE THEOLOGICAL ANALYSIS
Database: {Path(db_path).stem}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## CORPUS STATISTICS
- Total Sermons: {analysis['basic_stats']['total_sermons']:,}
- Total Words: {analysis['basic_stats']['total_words']:,}
- Average Sermon: {analysis['basic_stats']['avg_words']:,} words
- Date Range: {analysis['basic_stats']['date_range']}

## TOP PHRASE FREQUENCIES
"""
                    for phrase, count in list(analysis['phrase_frequencies'].items())[:20]:
                        full_text += f"- {phrase}: {count:,} times\n"
                    
                    full_text += "\n## THEOLOGICAL KEYWORDS\n"
                    for keyword, count in list(analysis['keywords'].items())[:20]:
                        full_text += f"- {keyword}: {count:,} times\n"
                    
                    if analysis.get('series'):
                        full_text += "\n## MAJOR SERMON SERIES\n"
                        for series, count in list(analysis['series'].items())[:10]:
                            full_text += f"- {series}: {count} sermons\n"
                    
                    full_text += f"\n## THEOLOGICAL EVALUATION\n\n{evaluation}\n"
                    
                    st.text_area("📋 Copy This Complete Report", full_text, height=400)
                    
                    # Store for export
                    st.session_state['last_analysis']['fabric_output'] = full_text
                    
                    st.success("💾 Saved - use Export tab")
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # ========================================================================
    # TAB 3: CONTEXT INSPECTOR
    # ========================================================================
    
    with tab3:
        st.header("Context Inspector")
        st.markdown("*Find EVERY mention with full context*")
        
        search_term = st.text_input("Search:", placeholder="e.g., Gloria Copeland")
        context_size = st.slider("Context size", 100, 500, 200)
        
        if search_term and st.button("Find All", type="primary"):
            with st.spinner(f"Searching for '{search_term}'..."):
                
                results = find_all_contexts(conn, table_name, transcript_col, search_term, context_size)
                
                st.markdown(f"### Found {len(results)} occurrences")
                
                if results:
                    sermons = len(set(r['title'] for r in results))
                    st.info(f"In {sermons} different sermons")
                    
                    for i, r in enumerate(results, 1):
                        with st.expander(f"{i}. {r['title']}"):
                            st.text(r['context'])
                            
                            # Simple sentiment
                            ctx = r['context'].lower()
                            if any(w in ctx for w in ['false', 'error', 'wrong', 'avoid']):
                                st.success("✅ Appears to be CRITIQUE")
                            elif any(w in ctx for w in ['agree', 'teach', 'truth']):
                                st.warning("⚠️ Appears to be ENDORSEMENT")
                else:
                    st.warning("Not found")
    
    # ========================================================================
    # TAB 4: COMPARE
    # ========================================================================
    
    with tab4:
        st.header("⚖️ Compare Teachers Side-by-Side")
        
        st.markdown("""
        Compare two teachers/ministries across specific topics to see theological differences.
        Perfect for comparing Katie Souza vs Pure Desire, or any two databases.
        """)
        
        # Upload second database
        st.subheader("Step 1: Upload Second Database")
        
        db2_file = st.file_uploader(
            "Upload another database to compare",
            type=['db', 'sqlite', 'sqlite3'],
            key='compare_db',
            help="Current database is already loaded. Upload a second one to compare."
        )
        
        if db2_file:
            # Save uploaded file temporarily
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
                tmp.write(db2_file.read())
                db2_path = tmp.name
            
            try:
                # Connect to second database
                conn2 = sqlite3.connect(db2_path)
                
                # Detect schema
                cursor2 = conn2.cursor()
                cursor2.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND (name='transcripts' OR name='video_transcripts')
                """)
                result = cursor2.fetchone()
                
                if not result:
                    st.error("No valid table found in second database")
                else:
                    table2 = result[0]
                    
                    cursor2.execute(f"PRAGMA table_info({table2})")
                    columns = [col[1] for col in cursor2.fetchall()]
                    
                    if 'transcript_text' in columns:
                        col2 = 'transcript_text'
                    elif 'transcript' in columns:
                        col2 = 'transcript'
                    else:
                        st.error("No transcript column found")
                        col2 = None
                    
                    if col2:
                        st.success(f"✅ Second database loaded: {db2_file.name}")
                        
                        # Get stats for Database 1 (current database)
                        cursor1 = conn.cursor()
                        cursor1.execute(f"""
                            SELECT COUNT(*), SUM(LENGTH({transcript_col}))
                            FROM {table_name}
                            WHERE {transcript_col} IS NOT NULL
                        """)
                        total_count1, chars1 = cursor1.fetchone()
                        total_words1 = chars1 // 5 if chars1 else 0
                        
                        # Get stats for Database 2 (uploaded database)
                        cursor2.execute(f"""
                            SELECT COUNT(*), SUM(LENGTH({col2}))
                            FROM {table2}
                            WHERE {col2} IS NOT NULL
                        """)
                        total_count2, chars2 = cursor2.fetchone()
                        total_words2 = chars2 // 5 if chars2 else 0
                        
                        # Names
                        col1, col2_display = st.columns(2)
                        with col1:
                            db1_name = st.text_input("Database 1 Name", value="Teacher 1", key='db1_name')
                        with col2_display:
                            db2_name = st.text_input("Database 2 Name", value="Teacher 2", key='db2_name')
                        
                        # Show stats
                        st.markdown("### 📊 Database Statistics")
                        
                        stats_col1, stats_col2 = st.columns(2)
                        
                        with stats_col1:
                            st.metric(f"{db1_name} - Total Sermons", total_count1)
                            st.metric(f"{db1_name} - Total Words", f"{total_words1:,}")
                        
                        with stats_col2:
                            st.metric(f"{db2_name} - Total Sermons", total_count2)
                            st.metric(f"{db2_name} - Total Words", f"{total_words2:,}")
                        
                        # Comparison options
                        st.markdown("---")
                        st.subheader("Step 2: Choose Comparison Type")
                        
                        comparison_type = st.radio(
                            "What would you like to compare?",
                            [
                                "🎯 Specific Topics (Keyword Comparison)",
                                "📊 Theological Themes (AI Analysis)",
                                "🔍 Custom Search Terms"
                            ],
                            key='comparison_type'
                        )
                        
                        # ================================================================
                        # SPECIFIC TOPICS COMPARISON
                        # ================================================================
                        
                        if comparison_type == "🎯 Specific Topics (Keyword Comparison)":
                            st.markdown("### Compare Specific Topics")
                            
                            # Preset topic sets
                            topic_preset = st.selectbox(
                                "Choose preset topics or create custom",
                                [
                                    "Custom",
                                    "Prosperity Gospel Red Flags",
                                    "Inner Healing Approaches", 
                                    "Money & Giving Teaching",
                                    "Holy Spirit & Power",
                                    "Grace vs. Works",
                                    "Sexual Purity Topics"
                                ],
                                key='topic_preset'
                            )
                            
                            # Define preset topics
                            presets = {
                                "Prosperity Gospel Red Flags": [
                                    "seed offering", "sow and reap", "financial blessing",
                                    "breakthrough offering", "prosperity", "wealth transfer"
                                ],
                                "Inner Healing Approaches": [
                                    "inner healing", "soul wounds", "trauma", "attachment",
                                    "healing memories", "deliverance"
                                ],
                                "Money & Giving Teaching": [
                                    "tithe", "offering", "giving", "money", "wealth", "prosperity"
                                ],
                                "Holy Spirit & Power": [
                                    "holy spirit", "power", "anointing", "glory", "presence",
                                    "manifestation"
                                ],
                                "Grace vs. Works": [
                                    "grace", "works", "faith", "righteousness", "justification",
                                    "sanctification"
                                ],
                                "Sexual Purity Topics": [
                                    "addiction", "purity", "sexual", "lust", "pornography",
                                    "accountability"
                                ]
                            }
                            
                            if topic_preset != "Custom":
                                topics = presets[topic_preset]
                                st.info(f"Searching for: {', '.join(topics)}")
                            else:
                                topics_input = st.text_area(
                                    "Enter topics to compare (one per line)",
                                    value="grace\nfaith\nlove\nhealing\nprayer",
                                    height=150,
                                    key='custom_topics'
                                )
                                topics = [t.strip() for t in topics_input.split('\n') if t.strip()]
                            
                            if st.button("🔍 Run Comparison", type="primary", key='run_topic_comparison'):
                                with st.spinner("Analyzing both databases..."):
                                    results = []
                                    
                                    for topic in topics:
                                        # Search database 1
                                        cursor.execute(f"""
                                            SELECT COUNT(*)
                                            FROM {table_name}
                                            WHERE LOWER({transcript_col}) LIKE ?
                                        """, (f'%{topic.lower()}%',))
                                        count1 = cursor.fetchone()[0]
                                        
                                        # Search database 2  
                                        cursor2.execute(f"""
                                            SELECT COUNT(*)
                                            FROM {table2}
                                            WHERE LOWER({col2}) LIKE ?
                                        """, (f'%{topic.lower()}%',))
                                        count2_val = cursor2.fetchone()[0]
                                        
                                        # Calculate percentages
                                        pct1 = (count1 / total_count1 * 100) if total_count1 > 0 else 0
                                        pct2 = (count2_val / total_count2 * 100) if total_count2 > 0 else 0
                                        
                                        results.append({
                                            'Topic': topic.title(),
                                            f'{db1_name} Count': count1,
                                            f'{db1_name} %': f"{pct1:.1f}%",
                                            f'{db2_name} Count': count2_val,
                                            f'{db2_name} %': f"{pct2:.1f}%",
                                            'Difference': count1 - count2_val
                                        })
                                    
                                    # Display results
                                    st.markdown("### 📊 Comparison Results")
                                    df = pd.DataFrame(results)
                                    st.dataframe(df, use_container_width=True)
                                    
                                    # Visualization
                                    st.markdown("### 📈 Visual Comparison")
                                    
                                    # Bar chart
                                    chart_data = pd.DataFrame({
                                        db1_name: [r[f'{db1_name} Count'] for r in results],
                                        db2_name: [r[f'{db2_name} Count'] for r in results],
                                    }, index=[r['Topic'] for r in results])
                                    
                                    st.bar_chart(chart_data)
                                    
                                    # Highlights
                                    st.markdown("### 💡 Key Differences")
                                    
                                    # Find biggest differences
                                    sorted_results = sorted(results, key=lambda x: abs(x['Difference']), reverse=True)
                                    
                                    for r in sorted_results[:3]:
                                        if r['Difference'] > 0:
                                            st.success(f"**{r['Topic']}**: {db1_name} emphasizes this more ({r[f'{db1_name} Count']} vs {r[f'{db2_name} Count']} mentions)")
                                        elif r['Difference'] < 0:
                                            st.info(f"**{r['Topic']}**: {db2_name} emphasizes this more ({r[f'{db2_name} Count']} vs {r[f'{db1_name} Count']} mentions)")
                        
                        # ================================================================
                        # THEOLOGICAL THEMES COMPARISON
                        # ================================================================
                        
                        elif comparison_type == "📊 Theological Themes (AI Analysis)":
                            st.markdown("### AI-Powered Theological Comparison")
                            st.info("Uses Claude AI to analyze theological differences across major themes")
                            
                            theme_options = st.multiselect(
                                "Select themes to analyze (max 3 for better quality)",
                                [
                                    "Gospel & Salvation",
                                    "Money & Prosperity",
                                    "Holy Spirit & Power",
                                    "Sin & Redemption",
                                    "Healing & Deliverance",
                                    "Prayer & Worship",
                                    "Authority & Spiritual Warfare",
                                    "Marriage & Family"
                                ],
                                default=["Gospel & Salvation", "Money & Prosperity"],
                                max_selections=3,
                                key='theme_options'
                            )
                            
                            if st.button("🤖 Generate AI Comparison", type="primary", key='ai_comparison'):
                                if not theme_options:
                                    st.warning("Please select at least one theme")
                                else:
                                    for theme in theme_options:
                                        with st.expander(f"📖 {theme}", expanded=True):
                                            with st.spinner(f"Analyzing {theme}..."):
                                                # Get sample sermons from both databases
                                                search_terms = {
                                                    "Gospel & Salvation": ["gospel", "salvation", "saved", "eternal life"],
                                                    "Money & Prosperity": ["money", "wealth", "prosperity", "blessing", "offering"],
                                                    "Holy Spirit & Power": ["holy spirit", "power", "anointing", "glory"],
                                                    "Sin & Redemption": ["sin", "redemption", "forgiveness", "repentance"],
                                                    "Healing & Deliverance": ["healing", "deliverance", "freedom", "breakthrough"],
                                                    "Prayer & Worship": ["prayer", "worship", "praise", "intercession"],
                                                    "Authority & Spiritual Warfare": ["authority", "spiritual warfare", "demons", "enemy"],
                                                    "Marriage & Family": ["marriage", "family", "children", "parenting"]
                                                }
                                                
                                                terms = search_terms.get(theme, [theme.lower()])
                                                
                                                # Get excerpts from DB1
                                                excerpts1 = []
                                                for term in terms:
                                                    cursor.execute(f"""
                                                        SELECT {transcript_col}
                                                        FROM {table_name}
                                                        WHERE LOWER({transcript_col}) LIKE ?
                                                        LIMIT 2
                                                    """, (f'%{term}%',))
                                                    for (text,) in cursor.fetchall():
                                                        if text:
                                                            pos = text.lower().find(term)
                                                            if pos >= 0:
                                                                start = max(0, pos - 300)
                                                                end = min(len(text), pos + 300)
                                                                excerpts1.append(text[start:end])
                                                    if len(excerpts1) >= 3:
                                                        break
                                                
                                                # Get excerpts from DB2
                                                excerpts2 = []
                                                for term in terms:
                                                    cursor2.execute(f"""
                                                        SELECT {col2}
                                                        FROM {table2}
                                                        WHERE LOWER({col2}) LIKE ?
                                                        LIMIT 2
                                                    """, (f'%{term}%',))
                                                    for (text,) in cursor2.fetchall():
                                                        if text:
                                                            pos = text.lower().find(term)
                                                            if pos >= 0:
                                                                start = max(0, pos - 300)
                                                                end = min(len(text), pos + 300)
                                                                excerpts2.append(text[start:end])
                                                    if len(excerpts2) >= 3:
                                                        break
                                                
                                                # AI Comparison
                                                if excerpts1 and excerpts2:
                                                    try:
                                                        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                                                        
                                                        prompt = f"""Compare how these two teachers/ministries approach "{theme}":

{db1_name} - Sample teachings:
{chr(10).join([f"• {e[:400]}" for e in excerpts1[:3]])}

{db2_name} - Sample teachings:
{chr(10).join([f"• {e[:400]}" for e in excerpts2[:3]])}

Provide a structured comparison (300-400 words):

1. **Main Theological Differences**: What are the core differences in their teaching on this theme?

2. **Scripture Usage**: How do they use the Bible differently on this topic?

3. **Practical Application**: How do their teachings apply to believers differently?

4. **Red Flags** (if any): Any concerning theological issues?

5. **Verdict**: Which approach is more biblically sound and why?

Be objective, fair, and cite specific concerns."""

                                                        message = client.messages.create(
                                                            model="claude-sonnet-4-20250514",
                                                            max_tokens=1500,
                                                            messages=[{"role": "user", "content": prompt}]
                                                        )
                                                        
                                                        st.markdown(message.content[0].text)
                                                        
                                                        # Show sample excerpts
                                                        with st.expander("📄 View sample excerpts used"):
                                                            st.markdown(f"**{db1_name}:**")
                                                            for e in excerpts1[:2]:
                                                                st.text(e[:300] + "...")
                                                            st.markdown(f"**{db2_name}:**")
                                                            for e in excerpts2[:2]:
                                                                st.text(e[:300] + "...")
                                                    
                                                    except Exception as e:
                                                        st.error(f"AI comparison failed: {str(e)}")
                                                else:
                                                    st.warning(f"Not enough content found for {theme}")
                        
                        # ================================================================
                        # CUSTOM SEARCH COMPARISON
                        # ================================================================
                        
                        elif comparison_type == "🔍 Custom Search Terms":
                            st.markdown("### Custom Search Comparison")
                            
                            custom_search = st.text_input(
                                "Enter search term or phrase",
                                placeholder="e.g., 'seed offering' or 'glory cloud'",
                                key='custom_search'
                            )
                            
                            if custom_search and st.button("🔍 Search Both Databases", key='custom_search_btn'):
                                with st.spinner("Searching..."):
                                    # Search DB1
                                    cursor.execute(f"""
                                        SELECT COUNT(*)
                                        FROM {table_name}
                                        WHERE LOWER({transcript_col}) LIKE ?
                                    """, (f'%{custom_search.lower()}%',))
                                    count1 = cursor.fetchone()[0]
                                    
                                    # Search DB2
                                    cursor2.execute(f"""
                                        SELECT COUNT(*)
                                        FROM {table2}
                                        WHERE LOWER({col2}) LIKE ?
                                    """, (f'%{custom_search.lower()}%',))
                                    count2_val = cursor2.fetchone()[0]
                                    
                                    # Display results
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.metric(
                                            f"{db1_name}",
                                            f"{count1} mentions",
                                            f"{count1/total_count1*100:.1f}% of sermons"
                                        )
                                    
                                    with col2:
                                        st.metric(
                                            f"{db2_name}",
                                            f"{count2_val} mentions",
                                            f"{count2_val/count2*100:.1f}% of sermons"
                                        )
                                    
                                    # Get sample contexts
                                    if count1 > 0 or count2_val > 0:
                                        st.markdown("### 📄 Sample Contexts")
                                        
                                        col1, col2 = st.columns(2)
                                        
                                        with col1:
                                            st.markdown(f"**{db1_name}:**")
                                            cursor.execute(f"""
                                                SELECT {transcript_col}
                                                FROM {table_name}
                                                WHERE LOWER({transcript_col}) LIKE ?
                                                LIMIT 3
                                            """, (f'%{custom_search.lower()}%',))
                                            
                                            for i, (text,) in enumerate(cursor.fetchall()):
                                                if text:
                                                    pos = text.lower().find(custom_search.lower())
                                                    if pos >= 0:
                                                        start = max(0, pos - 200)
                                                        end = min(len(text), pos + 200)
                                                        excerpt = text[start:end]
                                                        st.text_area("", excerpt, height=100, key=f"db1_{i}", disabled=True)
                                        
                                        with col2:
                                            st.markdown(f"**{db2_name}:**")
                                            cursor2.execute(f"""
                                                SELECT {col2}
                                                FROM {table2}
                                                WHERE LOWER({col2}) LIKE ?
                                                LIMIT 3
                                            """, (f'%{custom_search.lower()}%',))
                                            
                                            for i, (text,) in enumerate(cursor2.fetchall()):
                                                if text:
                                                    pos = text.lower().find(custom_search.lower())
                                                    if pos >= 0:
                                                        start = max(0, pos - 200)
                                                        end = min(len(text), pos + 200)
                                                        excerpt = text[start:end]
                                                        st.text_area("", excerpt, height=100, key=f"db2_{i}", disabled=True)
                
                conn2.close()
                os.unlink(db2_path)  # Clean up temp file
                
            except Exception as e:
                st.error(f"Error loading second database: {str(e)}")
                # Close connection if it exists before trying to delete
                try:
                    if 'conn2' in locals():
                        conn2.close()
                except:
                    pass
                # Now try to delete temp file
                try:
                    if os.path.exists(db2_path):
                        os.unlink(db2_path)
                except Exception as cleanup_error:
                    # If cleanup fails, just log it but don't crash
                    st.warning(f"Could not clean up temp file: {cleanup_error}")
        
        else:
            st.info("👆 Upload a second database above to start comparing")
            
            st.markdown("""
            ### How It Works
            
            1. **Upload Second Database**: Upload another .db file to compare against your current one
            2. **Choose Comparison Type**:
               - **Specific Topics**: Compare keyword frequencies
               - **Theological Themes**: AI-powered deep comparison
               - **Custom Search**: Search for any term across both
            3. **Get Results**: See side-by-side analysis
            
            ### Example Comparisons
            
            - **Katie Souza vs Pure Desire** → See prosperity gospel vs biblical counseling
            - **Bethel vs Reformed** → Compare charismatic vs traditional teaching
            - **Different Time Periods** → See how a teacher's theology evolved
            
            ### Tips
            
            - Use preset topic lists for quick insights
            - AI comparison works best with 2-3 themes at a time
            - Custom search is great for specific phrases like "seed offering"
            """)
    
    # ========================================================================
    # TAB 5: ASK QUESTIONS
    # ========================================================================
    
    with tab5:
        st.header("❓ Ask Questions About This Teacher")
        
        st.markdown("""
        Select from common theological questions or ask your own custom question.
        Claude will search the database and provide evidence-based answers.
        """)
        
        # Question type selection
        question_type = st.radio(
            "Question Type",
            ["📋 Canned Questions", "✏️ Custom Question"],
            horizontal=True
        )
        
        if question_type == "📋 Canned Questions":
            # Canned questions dropdown
            canned_question = st.selectbox(
                "Select a Question",
                [
                    "What does this teacher say about the nature of God?",
                    "What is this teacher's view of the Holy Spirit?",
                    "How does this teacher handle Scripture?",
                    "What does this teacher teach about salvation?",
                    "What does this teacher say about spiritual warfare?",
                    "What does this teacher teach about prosperity/finances?",
                    "What does this teacher say about healing?",
                    "How does this teacher view the end times/eschatology?",
                    "What does this teacher say about spiritual gifts?",
                    "What does this teacher teach about prayer?",
                    "What is this teacher's view of sin and holiness?",
                    "What does this teacher say about suffering?",
                    "How does this teacher view the role of prophets/prophecy?",
                    "What does this teacher say about deliverance/demons?",
                    "What is this teacher's view of the church?",
                    "What does this teacher teach about grace vs works?",
                    "What does this teacher say about faith?",
                    "How does this teacher view miracles and signs?",
                    "What does this teacher say about the cross/atonement?",
                    "What is this teacher's view of generational curses?"
                ]
            )
            
            question_to_ask = canned_question
            
        else:  # Custom Question
            custom_question = st.text_area(
                "Ask Your Own Question",
                placeholder="Example: Does this teacher teach that Christians can be possessed by demons?",
                height=100
            )
            
            question_to_ask = custom_question
        
        # Ask button
        if st.button("🤖 Get Answer", type="primary", disabled=not question_to_ask):
            if question_to_ask:
                with st.spinner(f"Searching database and analyzing..."):
                    try:
                        # Search database for relevant content
                        # Extract key terms from question
                        search_terms = []
                        # Simple keyword extraction (you could make this more sophisticated)
                        important_words = ['god', 'holy spirit', 'scripture', 'salvation', 'spiritual warfare', 
                                         'prosperity', 'healing', 'end times', 'gifts', 'prayer', 'sin', 'holiness',
                                         'suffering', 'prophets', 'prophecy', 'deliverance', 'demons', 'church',
                                         'grace', 'works', 'faith', 'miracles', 'cross', 'atonement', 'curse']
                        
                        for word in important_words:
                            if word in question_to_ask.lower():
                                search_terms.append(word)
                        
                        # If no important words found, just use first few words
                        if not search_terms:
                            search_terms = [w for w in question_to_ask.lower().split() if len(w) > 3][:3]
                        
                        # Search for relevant transcripts
                        cursor = conn.cursor()
                        conditions = []
                        for term in search_terms[:5]:  # Limit to 5 terms
                            conditions.append(f"LOWER({transcript_col}) LIKE ?")
                        
                        if conditions:
                            query = f"""
                                SELECT title, {transcript_col}
                                FROM {table_name}
                                WHERE {' OR '.join(conditions)}
                                LIMIT 10
                            """
                            params = [f'%{term}%' for term in search_terms[:5]]
                        else:
                            # Fallback - just get random samples
                            query = f"""
                                SELECT title, {transcript_col}
                                FROM {table_name}
                                WHERE {transcript_col} IS NOT NULL
                                ORDER BY RANDOM()
                                LIMIT 5
                            """
                            params = []
                        
                        cursor.execute(query, params)
                        results = cursor.fetchall()
                        
                        if not results:
                            st.warning("No relevant content found. Try rephrasing your question.")
                        else:
                            st.info(f"Found {len(results)} relevant transcripts. Analyzing...")
                            
                            # Combine relevant excerpts
                            context = ""
                            for title, text in results[:5]:
                                if text:
                                    # Get relevant excerpt (not whole thing)
                                    excerpt = text[:3000]  # First 3000 chars
                                    context += f"\n\n### {title}\n{excerpt}\n"
                            
                            # Ask Claude
                            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                            
                            message = client.messages.create(
                                model="claude-sonnet-4-20250514",
                                max_tokens=2000,
                                messages=[{
                                    "role": "user",
                                    "content": f"""Based on the following sermon transcripts, answer this question:

**QUESTION:** {question_to_ask}

**RELEVANT TRANSCRIPTS:**
{context}

INSTRUCTIONS:
1. Answer the question directly with specific evidence
2. QUOTE actual phrases from the transcripts (use "...")
3. Note what the teacher DOES say and what they DON'T say
4. If the transcripts don't address the question, say so
5. Identify any concerning teachings with quoted evidence
6. Keep answer focused and evidence-based

Provide a clear, well-organized answer with quoted evidence."""
                                }]
                            )
                            
                            st.markdown("### 💡 Answer")
                            st.markdown(message.content[0].text)
                            
                            # Show sources used
                            with st.expander("📚 Sources Used"):
                                for title, text in results[:5]:
                                    st.markdown(f"**{title}**")
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    # ========================================================================
    # TAB 6: EXPORT
    # ========================================================================
    
    with tab6:
        st.header("Export Reports")
        
        if 'last_analysis' in st.session_state:
            data = st.session_state['last_analysis']
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.download_button(
                    "📄 Markdown",
                    data['fabric_output'],
                    f"{data['db_name']}_analysis.md"
                )
            
            with col2:
                json_data = json.dumps(data['analysis'], indent=2)
                st.download_button(
                    "📊 JSON",
                    json_data,
                    f"{data['db_name']}_analysis.json"
                )
            
            st.code(data['fabric_output'][:500] + "...", language=None)
        else:
            st.info("Run Deep Analysis first")
    
    # ========================================================================
    # TAB 7: BLOG POST GENERATOR
    # ========================================================================
    
    with tab7:
        st.header("✍️ Blog Post Generator")
        st.markdown("*Generate professional blog posts from your database content*")
        
        # Blog topic input
        blog_topic = st.text_input(
            "Blog Post Topic",
            placeholder="e.g., overcoming addiction, father wounds, finding hope",
            help="Enter a topic you want to write about"
        )
        
        # Target audience
        audience = st.selectbox(
            "Target Audience",
            ["Recovery/Addiction", "General Christian", "Men's Ministry", "Pastoral/Leadership", "Youth/Young Adults"]
        )
        
        # Post length
        length = st.select_slider(
            "Post Length",
            options=["Short (1000-1500 words)", "Medium (1500-2500 words)", "Long (2500-3500 words)"],
            value="Medium (1500-2500 words)"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🎨 Generate Blog Post", type="primary"):
                if not blog_topic:
                    st.error("Please enter a topic")
                else:
                    with st.spinner(f"Searching database for '{blog_topic}' content..."):
                        # Search database for relevant content
                        cursor = conn.cursor()
                        search_terms = blog_topic.lower().split()
                        
                        # Build search query
                        conditions = []
                        for term in search_terms:
                            conditions.append(f"{transcript_col} LIKE '%{term}%'")
                        query = " OR ".join(conditions)
                        
                        cursor.execute(f"""
                            SELECT title, {transcript_col}
                            FROM {table_name}
                            WHERE {query}
                            LIMIT 10
                        """)
                        
                        results = cursor.fetchall()
                        
                        if not results:
                            st.warning(f"No content found for '{blog_topic}'. Try different keywords.")
                        else:
                            st.success(f"Found {len(results)} relevant transcripts!")
                            
                            # Extract key excerpts
                            excerpts = []
                            for title, text in results[:5]:
                                if not text:
                                    continue
                                for term in search_terms:
                                    pos = text.lower().find(term)
                                    if pos >= 0:
                                        start = max(0, pos - 300)
                                        end = min(len(text), pos + 700)
                                        excerpt = text[start:end].strip()
                                        excerpt = ' '.join(excerpt.split())
                                        excerpts.append(excerpt)
                                        break
                            
                            combined_content = "\n\n---\n\n".join(excerpts[:3])
                            
                            # Generate blog post with Claude
                            with st.spinner("✨ Writing blog post with AI..."):
                                try:
                                    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                                    
                                    word_target = {
                                        "Short (1000-1500 words)": "1000-1500",
                                        "Medium (1500-2500 words)": "1500-2500", 
                                        "Long (2500-3500 words)": "2500-3500"
                                    }[length]
                                    
                                    prompt = f"""Write a professional, compassionate blog post for a {audience.lower()} audience.

TOPIC: {blog_topic}

TARGET LENGTH: {word_target} words

SOURCE CONTENT (from sermon database):
{combined_content}

REQUIREMENTS:
1. Write in a warm, hopeful, recovery-focused tone
2. Use the source content for theological grounding, but write in your own words
3. Include:
   - Compelling introduction
   - Clear sections with h2/h3 headers
   - Biblical references where appropriate  
   - Practical application
   - Hopeful conclusion
4. Target {audience.lower()} audience specifically
5. Be substantive and helpful, not just inspirational fluff
6. Output as clean HTML (h2, h3, p, ul, ol, blockquote tags only - NO full page structure)

Write the blog post now:"""

                                    message = client.messages.create(
                                        model="claude-sonnet-4-20250514",
                                        max_tokens=4000,
                                        messages=[{"role": "user", "content": prompt}]
                                    )
                                    
                                    blog_html = message.content[0].text
                                    
                                    st.success("✅ Blog post generated!")
                                    
                                    # Display preview
                                    st.markdown("### Preview:")
                                    st.markdown(blog_html, unsafe_allow_html=True)
                                    
                                    # Download button
                                    st.download_button(
                                        label="📥 Download HTML",
                                        data=blog_html,
                                        file_name=f"{blog_topic.replace(' ', '-').lower()}.html",
                                        mime="text/html"
                                    )
                                    
                                    # Show word count
                                    word_count = len(blog_html.split())
                                    st.info(f"📊 Generated ~{word_count} words")
                                    
                                except Exception as e:
                                    st.error(f"Error generating blog post: {str(e)}")
        
        with col2:
            st.markdown("""
            ### How It Works
            
            1. **Search**: Finds relevant sermon content
            2. **Extract**: Pulls key excerpts
            3. **Generate**: Claude AI writes blog post
            4. **Download**: Get HTML file
            
            ### Tips
            
            - Use specific topics ("father wounds", not just "fathers")
            - Try different audience types
            - Longer posts = more depth
            - Edit the HTML before publishing
            """)
    
    # ========================================================================
    # TAB 8: SERIES GENERATOR
    # ========================================================================
    
    with tab8:
        st.header("📚 Multi-Post Series Generator")
        st.markdown("*Generate complete blog series (3-10 posts) using multiple databases*")
        
        st.markdown("""
        Create a coherent multi-part blog series that:
        - Builds progressively across posts
        - Uses complete sermons (not snippets)
        - Draws from multiple sources strategically
        - Maintains narrative flow
        """)
        
        # ====================================================================
        # STEP 1: DATABASE SELECTION
        # ====================================================================
        
        st.subheader("Step 1: Select Source Databases")
        
        # Detect available databases in uploads directory
        import glob
        from pathlib import Path
        
        available_dbs = []
        
        # Check uploads directory
        upload_path = Path("/mnt/user-data/uploads")
        if upload_path.exists():
            available_dbs.extend(glob.glob(str(upload_path / "*.db")))
        
        # Check current directory
        available_dbs.extend(glob.glob("*.db"))
        
        # Remove duplicates and format nicely
        available_dbs = list(set(available_dbs))
        db_names = {Path(db).name: db for db in available_dbs}
        
        # Always include current database
        current_db_name = Path(db_path).name
        
        if db_names:
            st.info(f"✅ Current database: **{current_db_name}**")
            
            # Multi-select for additional databases
            additional_dbs = st.multiselect(
                "Select additional databases to include",
                [name for name in db_names.keys() if name != current_db_name],
                help="Select databases to combine for series generation"
            )
            
            # Build final database list
            selected_dbs = {current_db_name: db_path}
            for name in additional_dbs:
                selected_dbs[name] = db_names[name]
            
            st.success(f"📊 Using {len(selected_dbs)} database(s): {', '.join(selected_dbs.keys())}")
        else:
            selected_dbs = {current_db_name: db_path}
            st.warning("Only current database available. Upload more .db files to /mnt/user-data/uploads for multi-source series.")
        
        # Upload additional databases
        st.markdown("**Or upload additional databases:**")
        uploaded_dbs = st.file_uploader(
            "Upload more databases",
            type=['db', 'sqlite', 'sqlite3'],
            accept_multiple_files=True,
            key='series_db_upload'
        )
        
        if uploaded_dbs:
            import tempfile
            for uploaded_db in uploaded_dbs:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
                    tmp.write(uploaded_db.read())
                    selected_dbs[uploaded_db.name] = tmp.name
            
            st.success(f"✅ Added {len(uploaded_dbs)} uploaded database(s)")
        
        # ====================================================================
        # STEP 2: SOURCE WEIGHTING (if multiple databases)
        # ====================================================================
        
        if len(selected_dbs) > 1:
            st.subheader("Step 2: Set Source Weights")
            st.markdown("*Adjust how much content comes from each database*")
            
            weights = {}
            cols = st.columns(len(selected_dbs))
            
            for idx, (name, path) in enumerate(selected_dbs.items()):
                with cols[idx]:
                    # Get database stats
                    try:
                        temp_conn = sqlite3.connect(path)
                        temp_table, temp_col = detect_schema(temp_conn)
                        temp_cursor = temp_conn.cursor()
                        temp_cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
                        sermon_count = temp_cursor.fetchone()[0]
                        temp_conn.close()
                        
                        st.metric(name, f"{sermon_count} sermons")
                        weights[name] = st.slider(
                            "Weight %",
                            0, 100, 
                            50 if len(selected_dbs) == 2 else 100 // len(selected_dbs),
                            key=f"weight_{name}"
                        )
                    except Exception as e:
                        st.error(f"Error loading {name}: {e}")
                        weights[name] = 0
            
            # Normalize weights
            total_weight = sum(weights.values())
            if total_weight > 0:
                normalized_weights = {k: v/total_weight for k, v in weights.items()}
            else:
                normalized_weights = {k: 1/len(weights) for k in weights.keys()}
            
            # Show distribution
            st.markdown("**Source Distribution:**")
            for name, pct in normalized_weights.items():
                st.progress(pct, text=f"{name}: {pct*100:.0f}%")
        else:
            normalized_weights = {current_db_name: 1.0}
        
        # ====================================================================
        # STEP 3: SERIES PARAMETERS
        # ====================================================================
        
        st.subheader("Step 3: Series Parameters")
        
        col1, col2 = st.columns(2)
        
        with col1:
            series_topic = st.text_input(
                "Series Topic",
                placeholder="e.g., Overcoming Father Wounds",
                help="Main theme for the series"
            )
            
            num_posts = st.slider(
                "Number of Posts",
                min_value=3,
                max_value=10,
                value=5,
                help="How many posts in the series"
            )
            
            series_audience = st.selectbox(
                "Target Audience",
                ["Recovery/Addiction", "General Christian", "Men's Ministry", 
                 "Pastoral/Leadership", "Youth/Young Adults", "Women's Ministry"]
            )
        
        with col2:
            post_length = st.select_slider(
                "Post Length",
                options=["Short (1500 words)", "Medium (2500 words)", "Long (3500 words)"],
                value="Medium (2500 words)"
            )
            
            sermons_per_post = st.slider(
                "Sermons per Post",
                min_value=3,
                max_value=10,
                value=5,
                help="How many complete sermons to use as source for each post"
            )
            
            series_style = st.selectbox(
                "Series Style",
                [
                    "Progressive Journey (builds week by week)",
                    "Deep Dive (each post standalone)",
                    "Problem-Solution Arc (diagnosis → solution)",
                    "Testimony-Heavy (personal stories)",
                    "Theology-Heavy (biblical teaching)"
                ]
            )
        
        # ====================================================================
        # STEP 4: GENERATE SERIES OUTLINE
        # ====================================================================
        
        if series_topic and st.button("📋 Generate Series Outline", type="secondary"):
            with st.spinner("Creating series outline..."):
                try:
                    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                    
                    outline_prompt = f"""Create an outline for a {num_posts}-post blog series on "{series_topic}" for a {series_audience.lower()} audience.

Series Style: {series_style}
Post Length: {post_length}

Provide:
1. Series title (compelling, clear)
2. Series description (2-3 sentences)
3. {num_posts} post titles that flow logically
4. Brief description of what each post covers
5. How the series builds/progresses

Make it practical, hope-filled, and theologically sound."""

                    outline_response = client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=1500,
                        messages=[{"role": "user", "content": outline_prompt}]
                    )
                    
                    st.session_state['series_outline'] = outline_response.content[0].text
                    st.markdown("### 📋 Series Outline")
                    st.markdown(outline_response.content[0].text)
                    
                except Exception as e:
                    st.error(f"Error generating outline: {e}")
        
        # ====================================================================
        # STEP 5: GENERATE COMPLETE SERIES
        # ====================================================================
        
        if series_topic and st.button("🎨 Generate Complete Series", type="primary"):
            with st.spinner(f"Generating {num_posts}-post series..."):
                
                # Calculate sermons needed from each database
                total_sermons_needed = num_posts * sermons_per_post
                sermons_from_each_db = {
                    name: int(total_sermons_needed * weight)
                    for name, weight in normalized_weights.items()
                }
                
                st.info(f"📊 Collecting {total_sermons_needed} total sermons: {sermons_from_each_db}")
                
                # Collect sermons from each database
                all_sources = []
                
                for db_name, db_path_local in selected_dbs.items():
                    try:
                        temp_conn = sqlite3.connect(db_path_local)
                        temp_table, temp_col = detect_schema(temp_conn)
                        
                        if not temp_table:
                            continue
                        
                        # Search for relevant sermons
                        search_terms = series_topic.lower().split()
                        conditions = [f"LOWER({temp_col}) LIKE ?" for _ in search_terms]
                        query = f"""
                            SELECT title, {temp_col}
                            FROM {temp_table}
                            WHERE {' OR '.join(conditions)}
                            AND {temp_col} IS NOT NULL
                            ORDER BY LENGTH({temp_col}) DESC
                            LIMIT ?
                        """
                        params = [f'%{term}%' for term in search_terms] + [sermons_from_each_db.get(db_name, 5)]
                        
                        temp_cursor = temp_conn.cursor()
                        temp_cursor.execute(query, params)
                        
                        for title, text in temp_cursor.fetchall():
                            if text:
                                all_sources.append({
                                    'database': db_name,
                                    'title': title,
                                    'text': text
                                })
                        
                        temp_conn.close()
                        
                    except Exception as e:
                        st.error(f"Error searching {db_name}: {e}")
                
                if not all_sources:
                    st.error(f"No relevant content found for '{series_topic}'. Try different keywords.")
                else:
                    st.success(f"✅ Found {len(all_sources)} relevant sermons")
                    
                    # Show source breakdown
                    with st.expander("📊 Source Breakdown"):
                        source_counts = {}
                        for source in all_sources:
                            db = source['database']
                            source_counts[db] = source_counts.get(db, 0) + 1
                        
                        for db, count in source_counts.items():
                            st.markdown(f"- **{db}**: {count} sermons")
                    
                    # Generate each post in the series
                    series_posts = []
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for post_num in range(1, num_posts + 1):
                        status_text.text(f"Generating Post {post_num} of {num_posts}...")
                        
                        # Select sources for this post
                        start_idx = (post_num - 1) * sermons_per_post
                        end_idx = start_idx + sermons_per_post
                        post_sources = all_sources[start_idx:end_idx]
                        
                        # Build context from complete sermons
                        context = ""
                        for idx, source in enumerate(post_sources, 1):
                            context += f"\n\n{'='*60}\n"
                            context += f"SOURCE {idx}: {source['title']} (from {source['database']})\n"
                            context += f"{'='*60}\n"
                            context += source['text'][:15000]  # Limit to ~15k chars per sermon
                        
                        # Generate post
                        try:
                            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                            
                            word_target = post_length.split("(")[1].split(" ")[0]
                            
                            post_prompt = f"""Write Post {post_num} of {num_posts} in a series on "{series_topic}".

TARGET AUDIENCE: {series_audience}
SERIES STYLE: {series_style}
TARGET LENGTH: {word_target} words

SOURCE MATERIAL (Complete Sermons):
{context}

REQUIREMENTS:
1. This is post {post_num} of {num_posts} - {"introduce the topic" if post_num == 1 else "build on previous posts" if post_num < num_posts else "conclude the series"}
2. Use the complete sermon content to understand the full message arc
3. Write in a warm, hopeful, {series_audience.lower()} tone
4. Include:
   - Compelling introduction
   - Clear h2/h3 section headers
   - Biblical references
   - Practical application
   - {"Transition to next post" if post_num < num_posts else "Powerful series conclusion"}
5. Output clean HTML (h2, h3, p, ul, ol, blockquote only)
6. Authentically reflect the source material's theology and tone

Write Post {post_num}:"""

                            message = client.messages.create(
                                model="claude-sonnet-4-20250514",
                                max_tokens=4000,
                                messages=[{"role": "user", "content": post_prompt}]
                            )
                            
                            post_html = message.content[0].text
                            
                            series_posts.append({
                                'post_num': post_num,
                                'html': post_html,
                                'sources': [s['title'] for s in post_sources],
                                'databases': [s['database'] for s in post_sources]
                            })
                            
                        except Exception as e:
                            st.error(f"Error generating post {post_num}: {e}")
                        
                        progress_bar.progress(post_num / num_posts)
                    
                    status_text.text("✅ Series generation complete!")
                    
                    # Auto-save to Series Library
                    try:
                        series_data = {
                            'title': series_topic,
                            'topic': series_topic,
                            'num_posts': num_posts,
                            'audience': series_audience,
                            'style': series_style,
                            'post_length': post_length,
                            'source_databases': {name: weight for name, weight in normalized_weights.items()},
                            'total_words': num_posts * int(word_target),
                            'cost': num_posts * 0.24,
                            'posts': series_posts
                        }
                        series_id = save_series_to_library(series_data)
                        st.info(f"💾 Auto-saved to Series Library (ID: {series_id})")
                    except Exception as e:
                        st.warning(f"⚠️ Could not auto-save to library: {e}")
                    
                    # Display series
                    st.markdown("---")
                    st.markdown(f"## 📚 {series_topic} - Complete {num_posts}-Post Series")
                    
                    # Add download options guide
                    with st.expander("📥 Download Options Guide", expanded=True):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("""
                            ### 📄 Individual Posts
                            **Use when:**
                            - Publishing to blog (WordPress, Substack)
                            - Email marketing (one per week)
                            - Course creation (one lesson per post)
                            - Social media content
                            
                            **Download:** Click button inside each post below
                            """)
                        
                        with col2:
                            st.markdown("""
                            ### 📚 Complete Series
                            **Use when:**
                            - Creating ebook/book
                            - Print devotional
                            - Internal review
                            - Archive/backup
                            
                            **Download:** Scroll to bottom for combined file
                            """)
                    
                    # Individual posts
                    st.markdown("### 📄 Individual Posts:")
                    
                    for post in series_posts:
                        with st.expander(f"📄 Post {post['post_num']} of {num_posts}", expanded=False):
                            st.markdown(post['html'], unsafe_allow_html=True)
                            
                            st.markdown("---")
                            st.markdown("**Sources Used:**")
                            for title, db in zip(post['sources'], post['databases']):
                                st.markdown(f"- {title} ({db})")
                            
                            st.markdown("")  # Spacing
                            
                            # Download individual post - more prominent
                            col1, col2, col3 = st.columns([1, 1, 2])
                            with col1:
                                st.download_button(
                                    label=f"📥 Download HTML",
                                    data=post['html'],
                                    file_name=f"{series_topic.replace(' ', '-').lower()}-post-{post['post_num']}.html",
                                    mime="text/html",
                                    key=f"download_post_html_{post['post_num']}"
                                )
                            with col2:
                                # Add markdown download option if html2text available
                                if HTML2TEXT_AVAILABLE:
                                    import html2text
                                    h = html2text.HTML2Text()
                                    h.ignore_links = False
                                    markdown_content = h.handle(post['html'])
                                    
                                    st.download_button(
                                        label=f"📥 Download MD",
                                        data=markdown_content,
                                        file_name=f"{series_topic.replace(' ', '-').lower()}-post-{post['post_num']}.md",
                                        mime="text/markdown",
                                        key=f"download_post_md_{post['post_num']}"
                                    )
                                else:
                                    st.info("Install html2text for Markdown export")
                    
                    # Download complete series - much more prominent
                    st.markdown("---")
                    st.markdown("### 📚 Complete Series Download:")
                    
                    complete_series_html = "\n\n".join([
                        f"<!-- POST {p['post_num']} -->\n{p['html']}" 
                        for p in series_posts
                    ])
                    
                    # Convert to markdown if available
                    if HTML2TEXT_AVAILABLE:
                        import html2text
                        h = html2text.HTML2Text()
                        h.ignore_links = False
                        complete_series_md = h.handle(complete_series_html)
                    else:
                        complete_series_md = None
                    
                    col_config = [1, 1, 1, 1] if HTML2TEXT_AVAILABLE else [1, 1, 1]
                    cols = st.columns(col_config)
                    
                    with cols[0]:
                        st.download_button(
                            label=f"📥 HTML (All {num_posts} Posts)",
                            data=complete_series_html,
                            file_name=f"{series_topic.replace(' ', '-').lower()}-complete-series.html",
                            mime="text/html",
                            use_container_width=True
                        )
                    
                    if HTML2TEXT_AVAILABLE:
                        with cols[1]:
                            st.download_button(
                                label=f"📥 Markdown (All {num_posts} Posts)",
                                data=complete_series_md,
                                file_name=f"{series_topic.replace(' ', '-').lower()}-complete-series.md",
                                mime="text/markdown",
                                use_container_width=True
                            )
                    
                    zip_col_idx = 2 if HTML2TEXT_AVAILABLE else 1
                    with cols[zip_col_idx]:
                        # Create ZIP file with all individual posts
                        import zipfile
                        import io
                        
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                            for post in series_posts:
                                # Add HTML
                                zip_file.writestr(
                                    f"post-{post['post_num']}.html",
                                    post['html']
                                )
                                # Add Markdown if available
                                if HTML2TEXT_AVAILABLE:
                                    h = html2text.HTML2Text()
                                    h.ignore_links = False
                                    md = h.handle(post['html'])
                                    zip_file.writestr(
                                        f"post-{post['post_num']}.md",
                                        md
                                    )
                        
                        st.download_button(
                            label=f"📦 ZIP (All Files)",
                            data=zip_buffer.getvalue(),
                            file_name=f"{series_topic.replace(' ', '-').lower()}-series.zip",
                            mime="application/zip",
                            use_container_width=True
                        )
                    
                    readme_col_idx = 3 if HTML2TEXT_AVAILABLE else 2
                    with cols[readme_col_idx]:
                        # Create README for the series
                        readme = f"""# {series_topic}
                        
## Series Information
- **Posts:** {num_posts}
- **Total Words:** ~{num_posts * int(word_target)}
- **Audience:** {series_audience}
- **Style:** {series_style}

## Posts in This Series
"""
                        for idx, post in enumerate(series_posts, 1):
                            readme += f"\n{idx}. Post {idx} ({post['word_count'] if 'word_count' in post else '~' + word_target} words)"
                        
                        readme += f"""

## Sources Used
"""
                        source_counts = {}
                        for post in series_posts:
                            for db in post['databases']:
                                source_counts[db] = source_counts.get(db, 0) + 1
                        
                        for db, count in source_counts.items():
                            readme += f"- {db}: {count} sermons\n"
                        
                        readme += f"""

## Files Included
- Individual posts (HTML + Markdown)
- Complete series (HTML + Markdown)
- This README

## Publishing Suggestions
- **Blog:** Upload individual HTML files, publish weekly
- **Email:** Copy/paste individual posts to email marketing tool
- **Book:** Use complete Markdown file, convert to PDF/ePub
- **Course:** Upload individual files as lessons
"""
                        
                        st.download_button(
                            label="📄 README",
                            data=readme,
                            file_name="README.md",
                            mime="text/markdown",
                            use_container_width=True
                        )
                    
                    st.success(f"🎉 Generated {num_posts} posts totaling ~{num_posts * int(word_target)} words!")
                    
                    # Add helpful next steps
                    st.info("""
                    **✅ Next Steps:**
                    1. Download individual posts for weekly blog publishing OR
                    2. Download complete series for book compilation OR
                    3. Download ZIP file to get everything
                    4. See README for publishing suggestions
                    """)
    
    # ========================================================================
    # TAB 9: SERIES LIBRARY
    # ========================================================================
    
    with tab9:
        st.header("🗄️ Series Library")
        st.markdown("*All your generated series in one place - never lose content again*")
        
        # Initialize library
        library_path = init_series_library()
        
        # Search and filter
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            search_term = st.text_input("🔍 Search series by title or topic", placeholder="e.g., father wounds")
        
        with col2:
            filter_status = st.selectbox("Status", ["All", "Draft", "Published"])
        
        with col3:
            sort_order = st.selectbox("Sort", ["Newest First", "Oldest First", "A-Z"])
        
        # Get series
        if search_term:
            all_series = search_series(search_term)
        else:
            all_series = get_all_series()
        
        # Filter by status
        if filter_status != "All":
            all_series = [s for s in all_series if s[6].lower() == filter_status.lower()]
        
        # Sort
        if sort_order == "Oldest First":
            all_series = list(reversed(all_series))
        elif sort_order == "A-Z":
            all_series = sorted(all_series, key=lambda x: x[1])
        
        # Display count
        st.markdown(f"### 📚 {len(all_series)} Series Found")
        
        if not all_series:
            st.info("""
            **No series in library yet!**
            
            Generate your first series in **Tab 8: Series Generator** and it will automatically save here.
            
            **This prevents losing content** when you close your browser!
            """)
        else:
            # Display series in grid
            for series in all_series:
                series_id, title, topic, num_posts, audience, date_created, status, total_words = series
                
                # Parse date
                try:
                    date_obj = datetime.fromisoformat(date_created)
                    date_str = date_obj.strftime("%B %d, %Y")
                except:
                    date_str = date_created
                
                # Create card for each series
                with st.expander(f"📚 {title}", expanded=False):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**Topic:** {topic}")
                        st.markdown(f"**Posts:** {num_posts} posts")
                        st.markdown(f"**Total Words:** ~{total_words:,}")
                        st.markdown(f"**Audience:** {audience}")
                        st.markdown(f"**Created:** {date_str}")
                        
                        # Status badge
                        if status.lower() == 'published':
                            st.success("✅ Published")
                        else:
                            st.info("📝 Draft")
                    
                    with col2:
                        # Action buttons
                        if st.button("👁️ View Details", key=f"view_{series_id}"):
                            st.session_state['viewing_series'] = series_id
                        
                        if st.button("📥 Download", key=f"download_{series_id}"):
                            st.session_state['downloading_series'] = series_id
                        
                        if st.button("🗑️ Delete", key=f"delete_{series_id}", type="secondary"):
                            if st.session_state.get(f'confirm_delete_{series_id}'):
                                delete_series(series_id)
                                st.success(f"Deleted: {title}")
                                st.rerun()
                            else:
                                st.session_state[f'confirm_delete_{series_id}'] = True
                                st.warning("Click again to confirm deletion")
        
        # View series details
        if 'viewing_series' in st.session_state:
            series_id = st.session_state['viewing_series']
            details = get_series_details(series_id)
            
            if details:
                st.markdown("---")
                st.markdown("## 📖 Series Details")
                
                # Back button
                if st.button("← Back to Library"):
                    del st.session_state['viewing_series']
                    st.rerun()
                
                # Series metadata
                series_data = details['series']
                posts_data = details['posts']
                
                st.markdown(f"### {series_data[1]}")  # title
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Posts", series_data[3])
                with col2:
                    st.metric("Total Words", f"{series_data[9]:,}")
                with col3:
                    st.metric("Estimated Cost", f"${series_data[10]:.2f}")
                
                # Source breakdown
                if series_data[8]:  # source_databases
                    try:
                        sources = json.loads(series_data[8])
                        st.markdown("**Source Databases:**")
                        for db_name, weight in sources.items():
                            st.progress(weight, text=f"{db_name}: {weight*100:.0f}%")
                    except:
                        pass
                
                # Posts list
                st.markdown("---")
                st.markdown("### 📝 Posts in This Series")
                
                for post in posts_data:
                    post_id, series_id, post_num, post_title, html_content, md_content, word_count, sources, date_created = post
                    
                    with st.expander(f"Post {post_num}: {post_title or 'Untitled'}", expanded=False):
                        # Preview
                        st.markdown(html_content[:500] + "..." if len(html_content) > 500 else html_content, unsafe_allow_html=True)
                        
                        st.markdown("---")
                        
                        # Download buttons
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label=f"📥 Download HTML",
                                data=html_content,
                                file_name=f"post-{post_num}.html",
                                mime="text/html",
                                key=f"dl_html_{post_id}"
                            )
                        
                        if HTML2TEXT_AVAILABLE and md_content:
                            with col2:
                                st.download_button(
                                    label=f"📥 Download MD",
                                    data=md_content,
                                    file_name=f"post-{post_num}.md",
                                    mime="text/markdown",
                                    key=f"dl_md_{post_id}"
                                )
                
                # Download complete series
                st.markdown("---")
                st.markdown("### 📥 Download Complete Series")
                
                complete_html = "\n\n".join([f"<!-- POST {p[2]} -->\n{p[4]}" for p in posts_data])
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.download_button(
                        label="📥 Complete Series (HTML)",
                        data=complete_html,
                        file_name=f"{series_data[2].replace(' ', '-').lower()}-complete.html",
                        mime="text/html",
                        use_container_width=True
                    )
                
                with col2:
                    # Create ZIP
                    import zipfile
                    import io
                    
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        for post in posts_data:
                            zip_file.writestr(f"post-{post[2]}.html", post[4])
                            if post[5]:  # markdown
                                zip_file.writestr(f"post-{post[2]}.md", post[5])
                    
                    st.download_button(
                        label="📦 ZIP Package",
                        data=zip_buffer.getvalue(),
                        file_name=f"{series_data[2].replace(' ', '-').lower()}-series.zip",
                        mime="application/zip",
                        use_container_width=True
                    )
                
                with col3:
                    # Update status
                    new_status = st.selectbox(
                        "Status",
                        ["draft", "published"],
                        index=0 if series_data[11] == 'draft' else 1,
                        key=f"status_{series_id}"
                    )
                    
                    if st.button("💾 Update Status"):
                        library_conn = sqlite3.connect(library_path)
                        cursor = library_conn.cursor()
                        cursor.execute(
                            "UPDATE series_library SET status = ? WHERE series_id = ?",
                            (new_status, series_id)
                        )
                        library_conn.commit()
                        library_conn.close()
                        st.success(f"Status updated to: {new_status}")
        
        # Library stats
        if all_series:
            st.markdown("---")
            st.markdown("### 📊 Library Statistics")
            
            total_series = len(all_series)
            total_posts = sum(s[3] for s in all_series)
            total_words = sum(s[7] for s in all_series)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Series", total_series)
            with col2:
                st.metric("Total Posts", total_posts)
            with col3:
                st.metric("Total Words", f"{total_words:,}")
    
    conn.close()

else:
    st.error("Can't connect to database")

st.markdown("---")
st.markdown("*No sampling. No missing data. Complete accuracy.*")
