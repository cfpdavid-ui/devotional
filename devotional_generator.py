#!/usr/bin/env python3
"""
Daily Devotional Generator
Generates 10 devotionals from ministry content databases
Output: Professional PDFs ready for distribution
"""

import streamlit as st
import sqlite3
import anthropic
from pathlib import Path
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.lib import colors
import io

# Page config
st.set_page_config(
    page_title="Daily Devotional Generator",
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get API key from Streamlit secrets
try:
    ANTHROPIC_API_KEY = st.secrets["ANTHROPIC_API_KEY"]
except:
    st.error("⚠️ API key not found! Add ANTHROPIC_API_KEY to Streamlit Secrets in app settings.")
    st.stop()

# ============================================================================
# DATABASE UTILITIES
# ============================================================================

def find_databases():
    """Find all available database files"""
    search_paths = [
        Path("/mnt/user-data/outputs"),
        Path("/mnt/user-data/uploads"),
        Path.cwd()
    ]
    
    databases = {}
    for path in search_paths:
        if path.exists():
            for db_file in path.glob("*.db"):
                if db_file.stem not in databases:
                    databases[db_file.stem] = str(db_file)
    
    return databases

def get_database_info(db_path):
    """Get basic info about a database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Detect table
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND (name='transcripts' OR name='video_transcripts')
        """)
        result = cursor.fetchone()
        
        if not result:
            return None
        
        table_name = result[0]
        
        # Get stats
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        # Get transcript column
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        transcript_col = 'transcript_text' if 'transcript_text' in columns else 'transcript'
        
        cursor.execute(f"SELECT SUM(LENGTH({transcript_col})) FROM {table_name}")
        total_chars = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            'table_name': table_name,
            'transcript_col': transcript_col,
            'count': count,
            'total_chars': total_chars
        }
    except Exception as e:
        st.error(f"Database error: {e}")
        return None

def get_sample_content(db_path, table_name, transcript_col, max_words=50000):
    """Get sample content from database for devotional generation"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get a good sampling of content
        cursor.execute(f"""
            SELECT {transcript_col} 
            FROM {table_name}
            ORDER BY RANDOM()
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        conn.close()
        
        # Combine and limit
        combined = "\n\n".join([s[0] for s in samples if s[0]])
        words = combined.split()
        
        if len(words) > max_words:
            combined = " ".join(words[:max_words])
        
        return combined
    except Exception as e:
        st.error(f"Error sampling content: {e}")
        return ""

# ============================================================================
# PDF GENERATION
# ============================================================================

def create_devotional_pdf(devotionals, output_path, source_name):
    """Create professional PDF with all devotionals"""
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )
    
    # Custom styles
    styles = getSampleStyleSheet()
    
    # Title style
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=12,
        alignment=TA_LEFT,
        fontName='Helvetica-Bold'
    )
    
    # Subtitle style
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#666666'),
        spaceAfter=20,
        alignment=TA_LEFT,
        fontName='Helvetica-Oblique'
    )
    
    # Body style
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=11,
        leading=16,
        alignment=TA_JUSTIFY,
        spaceAfter=12,
        fontName='Helvetica'
    )
    
    # Scripture style
    scripture_style = ParagraphStyle(
        'Scripture',
        parent=styles['BodyText'],
        fontSize=10,
        leading=14,
        textColor=colors.HexColor('#333333'),
        leftIndent=20,
        spaceAfter=6,
        fontName='Helvetica-Oblique'
    )
    
    # Section header style
    section_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=8,
        spaceBefore=12,
        fontName='Helvetica-Bold'
    )
    
    # Attribution style
    attribution_style = ParagraphStyle(
        'Attribution',
        parent=styles['BodyText'],
        fontSize=8,
        textColor=colors.HexColor('#999999'),
        alignment=TA_CENTER,
        fontName='Helvetica-Oblique'
    )
    
    # Build document
    story = []
    
    # Cover page
    story.append(Spacer(1, 1.5*inch))
    story.append(Paragraph("Daily Devotionals", title_style))
    story.append(Paragraph(f"10-Day Series", subtitle_style))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph(f"Source: {source_name}", attribution_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}", attribution_style))
    story.append(PageBreak())
    
    # Each devotional
    for i, dev in enumerate(devotionals, 1):
        # Day header
        story.append(Paragraph(f"Day {i}", section_style))
        story.append(Spacer(1, 6))
        
        # Title
        story.append(Paragraph(dev['title'], title_style))
        story.append(Spacer(1, 12))
        
        # Narrative paragraphs
        for para in dev['narrative']:
            story.append(Paragraph(para, body_style))
            story.append(Spacer(1, 6))
        
        story.append(Spacer(1, 12))
        
        # Key scriptures section
        story.append(Paragraph("Key Scriptures:", section_style))
        for scripture in dev['scriptures']:
            story.append(Paragraph(f"• {scripture}", scripture_style))
        
        story.append(Spacer(1, 20))
        
        # Notes section
        story.append(Paragraph("Personal Notes & Reflection:", section_style))
        
        # Create lined space for notes
        note_lines = []
        for _ in range(6):
            note_lines.append(['_' * 100])
        
        note_table = Table(note_lines, colWidths=[6.5*inch])
        note_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#cccccc')),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        
        story.append(note_table)
        story.append(Spacer(1, 20))
        
        # Attribution at bottom
        story.append(Paragraph(
            f"<i>Devotional synthesized from {source_name}</i>",
            attribution_style
        ))
        
        # Page break except for last devotional
        if i < len(devotionals):
            story.append(PageBreak())
    
    # Build PDF
    doc.build(story)
    
    # Get PDF data
    pdf_data = buffer.getvalue()
    buffer.close()
    
    return pdf_data

# ============================================================================
# AI GENERATION
# ============================================================================

def generate_devotionals(sample_content, topic, source_name):
    """Generate 10 devotionals using Claude"""
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = f"""You are creating 10 daily devotionals based on content from {source_name}.

TOPIC: {topic}

SOURCE CONTENT:
{sample_content[:40000]}

Generate exactly 10 devotionals following this structure:

FORMAT FOR EACH DEVOTIONAL:
---DEVOTIONAL START---
TITLE: [Engaging 3-8 word title]

NARRATIVE:
[2-3 paragraphs of teaching/reflection, 150-200 words total. Make it personal, practical, and encouraging. Draw from the source content but write in a devotional style.]

KEY SCRIPTURES:
- [Scripture reference 1 with brief context]
- [Scripture reference 2 with brief context]
- [Scripture reference 3 with brief context]
---DEVOTIONAL END---

REQUIREMENTS:
1. Each devotional should be self-contained but part of a 10-day journey
2. Vary the topics while staying within the overall theme
3. Make narratives personal and practical
4. Include 3 scripture references per devotional
5. Write in an encouraging, accessible tone
6. Draw genuine insights from the source material

Generate all 10 devotionals now, numbered 1-10."""

    with st.spinner("🤖 Generating 10 devotionals with Claude..."):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=16000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            content = response.content[0].text
            
            # Parse devotionals
            devotionals = []
            sections = content.split('---DEVOTIONAL START---')
            
            for section in sections[1:]:  # Skip first empty split
                if '---DEVOTIONAL END---' not in section:
                    continue
                
                section = section.split('---DEVOTIONAL END---')[0].strip()
                
                # Parse title
                title_match = section.split('TITLE:', 1)
                if len(title_match) < 2:
                    continue
                
                remaining = title_match[1]
                parts = remaining.split('NARRATIVE:', 1)
                if len(parts) < 2:
                    continue
                
                title = parts[0].strip()
                
                # Parse narrative
                narrative_section = parts[1].split('KEY SCRIPTURES:', 1)
                if len(narrative_section) < 2:
                    continue
                
                narrative_text = narrative_section[0].strip()
                narrative_paras = [p.strip() for p in narrative_text.split('\n\n') if p.strip()]
                
                # Parse scriptures
                scriptures_text = narrative_section[1].strip()
                scriptures = []
                for line in scriptures_text.split('\n'):
                    line = line.strip()
                    if line.startswith('-') or line.startswith('•'):
                        scripture = line[1:].strip()
                        if scripture:
                            scriptures.append(scripture)
                
                devotionals.append({
                    'title': title,
                    'narrative': narrative_paras,
                    'scriptures': scriptures
                })
            
            return devotionals
            
        except Exception as e:
            st.error(f"Error generating devotionals: {e}")
            return []

# ============================================================================
# MAIN APP
# ============================================================================

st.title("📖 Daily Devotional Generator")
st.markdown("Generate 10 professional devotionals from ministry teaching content")
st.markdown("---")

# Sidebar - Database selection
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Find databases
    databases = find_databases()
    
    if not databases:
        st.error("No databases found!")
        st.stop()
    
    # Database selection
    selected_db = st.selectbox(
        "Select Database",
        options=list(databases.keys()),
        format_func=lambda x: x.replace('_', ' ').title()
    )
    
    db_path = databases[selected_db]
    
    # Get database info
    db_info = get_database_info(db_path)
    
    if db_info:
        st.success(f"✅ Connected")
        st.metric("Sessions", db_info['count'])
        st.metric("Total Content", f"{db_info['total_chars']:,} chars")
    else:
        st.error("Invalid database")
        st.stop()
    
    st.markdown("---")
    
    # Generation settings
    st.subheader("Generation Settings")
    
    topic = st.text_input(
        "Topic/Theme",
        value="Hope and Redemption",
        help="Main theme for the 10-day devotional series"
    )
    
    source_attribution = st.text_input(
        "Source Attribution",
        value="Romans 12:2 Ministry",
        help="How to credit the source in PDFs"
    )

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Generate Devotionals")
    st.markdown("""
    This tool will:
    1. Sample content from the selected database
    2. Generate 10 unique devotionals on your chosen topic
    3. Create a professional PDF with space for notes
    4. Include proper source attribution
    """)

with col2:
    st.subheader("Output Format")
    st.markdown("""
    **Each devotional includes:**
    - Engaging title
    - 2-3 paragraph narrative
    - 3 key scriptures
    - Space for personal notes
    - Source attribution
    """)

st.markdown("---")

# Generation button
if st.button("🚀 Generate 10 Devotionals", type="primary", use_container_width=True):
    
    # Sample content from database
    with st.spinner("📚 Sampling content from database..."):
        sample_content = get_sample_content(
            db_path,
            db_info['table_name'],
            db_info['transcript_col']
        )
    
    if not sample_content:
        st.error("Could not sample content from database")
        st.stop()
    
    st.success(f"✅ Sampled {len(sample_content.split())} words from database")
    
    # Generate devotionals
    devotionals = generate_devotionals(sample_content, topic, source_attribution)
    
    if not devotionals:
        st.error("Failed to generate devotionals")
        st.stop()
    
    if len(devotionals) < 10:
        st.warning(f"Only generated {len(devotionals)} devotionals (expected 10)")
    else:
        st.success(f"✅ Generated {len(devotionals)} devotionals!")
    
    # Show preview
    st.markdown("---")
    st.subheader("📋 Preview")
    
    with st.expander("View Generated Devotionals", expanded=True):
        for i, dev in enumerate(devotionals, 1):
            st.markdown(f"### Day {i}: {dev['title']}")
            
            for para in dev['narrative']:
                st.markdown(para)
            
            st.markdown("**Key Scriptures:**")
            for scripture in dev['scriptures']:
                st.markdown(f"- {scripture}")
            
            st.markdown("---")
    
    # Generate PDF
    st.markdown("---")
    st.subheader("📥 Download PDF")
    
    with st.spinner("📄 Creating PDF..."):
        pdf_data = create_devotional_pdf(devotionals, None, source_attribution)
    
    filename = f"devotionals-{topic.lower().replace(' ', '-')}-{datetime.now().strftime('%Y%m%d')}.pdf"
    
    st.download_button(
        label="📥 Download Complete PDF",
        data=pdf_data,
        file_name=filename,
        mime="application/pdf",
        use_container_width=True
    )
    
    st.success(f"✅ PDF ready! ({len(pdf_data):,} bytes)")
    
    # Stats
    total_words = sum(len(' '.join(dev['narrative']).split()) for dev in devotionals)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Devotionals", len(devotionals))
    with col2:
        st.metric("Total Words", f"{total_words:,}")
    with col3:
        st.metric("Pages", "~20-25")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #999; font-size: 0.9em;'>
    <p>Devotional Generator • Proof of Concept</p>
    <p>Content sourced from ministry databases with proper attribution</p>
</div>
""", unsafe_allow_html=True)
