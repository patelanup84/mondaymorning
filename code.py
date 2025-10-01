# ============================================================================
# FOLDER STRUCTURE SUMMARY
# ============================================================================
"""
mondaymorning/
├── app.py                          # Flask app initialization
├── config.py                       # Environment configuration
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Container config
├── .env                            # Environment variables (gitignored)
├── .env.example                    # Environment template
├── .gitignore                      # Git ignore rules
├── README.md                       # Project documentation
│
├── data/                           # Database location
│   └── raw/
│       └── qp.db                   # Existing SQLite database
│
├── shared/                         # Cross-feature utilities
│   ├── __init__.py
│   ├── db.py                       # SQLAlchemy initialization
│   └── utils.py                    # Common helpers
│
├── features/                       # Feature modules (vertical slices)
│   ├── __init__.py
│   └── property_table/             # First feature
│       ├── __init__.py
│       ├── routes.py               # Presentation layer
│       ├── service.py              # Business layer
│       ├── models.py               # Data layer
│       └── templates/
│           └── property_table.html # UI template
│
├── static/                         # Static assets
│   ├── css/
│   │   └── styles.css
│   └── js/
│       └── app.js
│
└── tests/                          # Test files
    └── test_property_table.py
"""