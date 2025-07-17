#!/usr/bin/env python3
"""
Domain Checker and Business Identifier
Checks domain availability, follows redirects, and identifies legitimate business websites
Supports batch processing, progress saving, resume functionality, and real-time CSV output
ENHANCED VERSION: Added advanced vacation rental business classification
"""

import requests
import csv
import json
import time
import os
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import logging
import urllib3
import threading
from pathlib import Path

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DomainChecker:
    def __init__(self, timeout=8, max_workers=10, batch_size=50, enable_deep_crawl=True):
        self.timeout = timeout
        self.enable_deep_crawl = enable_deep_crawl
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Network connectivity tracking
        self.network_issues_count = 0
        self.consecutive_failures = 0
        self.last_connectivity_check = 0
        self.connectivity_lock = threading.Lock()
        
        # Progress tracking
        self.progress_file = None
        self.processed_domains = set()
        self.current_batch = 0
        self.total_batches = 0
        
        # Real-time CSV tracking
        self.csv_files = {}
        self.csv_writers = {}
        self.stats = {
            'total_processed': 0,
            'working': 0,
            'business': 0,
            'parked': 0,
            'failed': 0,
            'industries': {},
            'company_sizes': {},
            'target_customers': 0,
            'excluded_businesses': {},
            'start_time': None,
            'current_batch': 0,
            'total_batches': 0,
            # NEW STATS FIELDS
            'vr_business_models': {},
            'high_priority_targets': 0,
            'medium_priority_targets': 0,
            'decision_maker_accessible': 0,
            'website_needs_upgrade': 0,
            'vr_property_types': {}
        }
        
        # Simple SSL configuration
        try:
            import ssl
            from requests.adapters import HTTPAdapter
            from urllib3.util.ssl_ import create_urllib3_context
            
            class SSLAdapter(HTTPAdapter):
                def init_poolmanager(self, *args, **kwargs):
                    try:
                        ctx = create_urllib3_context()
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_NONE
                        try:
                            ctx.set_ciphers('DEFAULT')
                        except:
                            pass
                        kwargs['ssl_context'] = ctx
                    except Exception:
                        pass
                    return super().init_poolmanager(*args, **kwargs)
            
            self.session.mount('https://', SSLAdapter())
            
        except Exception as e:
            logger.warning(f"SSL adapter setup failed: {e}")
        
        # Enhanced parked domain indicators
        self.parked_indicators = [
            'domain for sale', 'buy this domain', 'parked domain', 'coming soon',
            'under construction', 'this domain is for sale', 'expired domain',
            'register this domain', 'domain parking', 'premium domain',
            'inquire about this domain', 'make an offer', 'domain auction',
            'brandable domain', 'great domain', 'perfect domain', 'domain available',
            'inquire now', 'buy now', 'purchase this domain', 'acquire this domain',
            'godaddy', 'namecheap', 'sedo', 'afternic', 'hugedomains', 'dan.com', 
            'escrow.com', 'flippa', 'brandpa', 'squadhelp', 'undeveloped',
            'domain.com', 'name.com', 'networksolutions', 'dynadot',
            'brandable.com', 'brandroot', 'domainhostingview', 'whois.net',
            'domainmarket', 'premiumdomains', 'brandbucket', 'namerific',
            'placeholder page', 'temporary page', 'site coming soon',
            'website coming soon', 'launching soon', 'site under development',
            'default page', 'apache2 debian default page', 'nginx default page',
            'it works!', 'apache2 ubuntu default page', 'welcome to nginx',
            'cpanel', 'whm', 'plesk', 'directadmin', 'hostgator', 'bluehost',
            'shared hosting', 'web hosting', 'hosting account', 'server default',
            'this domain is hosted by', 'powered by', 'hosted on',
            'this site is temporarily unavailable', 'account suspended',
            'domain suspended', 'hosting account suspended', 'service unavailable',
            'bandwidth limit exceeded', 'quota exceeded', 'site maintenance',
            'temporarily down', 'website offline', 'server error',
            'suspended domain', 'suspended account', 'terms of service violation',
            'directory listing', 'index of /', 'apache directory listing',
            'welcome to your new website', 'congratulations on your new domain',
            'this domain has been registered', 'domain successfully registered',
            'thank you for registering', 'domain registration successful',
            'business for sale', 'website for sale', 'established domain',
            'traffic included', 'seo optimized domain', 'keyword rich domain',
            'exact match domain', 'premium .com domain', 'valuable domain',
            'investment opportunity', 'revenue generating', 'monetized domain',
            'landing page', 'lead capture', 'affiliate marketing', 'monetization',
            'ppc ready', 'adsense ready', 'revenue potential', 'traffic value',
            'type-in traffic', 'direct navigation', 'category killer',
            '.gallery domain', '.ist domain', '.qa domain', 'new tld',
            'premium extension', 'new domain extension'
        ]
        
        # Enhanced company size indicators with more detailed analysis
        self.large_company_indicators = {
            'listing_platform_keywords': [
                'thousands of listings', 'millions of properties', 'search properties',
                'browse listings', 'property listings', 'rental listings', 'accommodation listings',
                'booking platform', 'reservation platform', 'marketplace', 'directory',
                'find properties', 'compare properties', 'property search engine',
                'listing database', 'property database', 'inventory of', 'catalog of'
            ],
            'headquarters_indicators': [
                'headquarters', 'corporate headquarters', 'global headquarters', 'hq',
                'head office', 'corporate office', 'main office', 'principal office',
                'executive offices', 'corporate campus', 'world headquarters'
            ],
            'fortune_keywords': [
                'fortune 500', 'fortune 1000', 'nasdaq', 'nyse', 'stock exchange',
                'publicly traded', 'shareholders', 'investor relations', 'sec filings',
                'annual report', 'quarterly earnings', 'market cap', 'ticker symbol',
                's&p 500', 'dow jones', 'public company', 'publicly held'
            ],
            'scale_indicators': [
                'global', 'worldwide', 'international', 'multinational', 'enterprise',
                'corporation', 'corporate', 'offices worldwide', 'global presence', 
                'international offices', 'regional offices', 'subsidiaries', 'divisions', 
                'business units', 'operating companies', 'franchise locations',
                'nationwide', 'countrywide', 'multi-state', 'multi-national'
            ],
            'size_keywords': [
                'thousands of employees', 'million employees', 'billion', 'millions of customers',
                'fortune', 'largest', 'leading provider', 'market leader', 'industry leader',
                'global leader', 'worldwide leader', 'established 18', 'established 19',
                'since 18', 'since 19', 'founded 18', 'founded 19', 'over 100 years',
                'billions in revenue', 'millions in revenue', 'market capitalization'
            ],
            'corporate_structure': [
                'chief executive officer', 'chief financial officer', 'chief technology officer',
                'board of directors', 'executive team', 'leadership team', 'senior management',
                'c-suite', 'executive committee', 'advisory board', 'board members',
                'vice president', 'senior vice president', 'executive vice president',
                'managing director', 'general manager', 'regional manager'
            ],
            'compliance_indicators': [
                'privacy policy', 'terms of service', 'cookie policy', 'gdpr', 'ccpa',
                'compliance', 'regulatory', 'sox compliance', 'iso certified',
                'quality management', 'certifications', 'accreditation', 'legal disclaimer',
                'terms and conditions', 'user agreement', 'service agreement'
            ],
            'enterprise_services': [
                'enterprise solutions', 'b2b', 'business solutions', 'enterprise grade',
                'scalable solutions', 'white paper', 'case studies', 'implementation',
                'professional services', 'consulting', 'support portal', 'api documentation',
                'enterprise clients', 'corporate clients', 'institutional clients'
            ],
            'big_business_indicators': [
                'press releases', 'media center', 'news room', 'newsroom', 'press center',
                'media kit', 'brand assets', 'corporate communications', 'public relations',
                'investor center', 'corporate governance', 'sustainability report',
                'corporate responsibility', 'annual reports', 'quarterly reports'
            ]
        }
        
        self.medium_company_indicators = {
            'regional_presence': [
                'regional', 'multi-location', 'multiple offices', 'branch offices',
                'locations', 'established', 'growing company', 'expanding',
                'several locations', 'multiple branches', 'regional service',
                'serving multiple cities', 'multiple markets'
            ],
            'professional_services': [
                'professional', 'certified', 'licensed', 'accredited', 'experienced team',
                'expert', 'specialists', 'consultants', 'advisors', 'professional staff',
                'qualified team', 'industry experts', 'certified professionals'
            ],
            'business_maturity': [
                'years of experience', 'established business', 'trusted partner',
                'proven track record', 'industry expertise', 'comprehensive services',
                'experienced company', 'established reputation', 'trusted provider'
            ],
            'medium_scale_indicators': [
                'regional leader', 'local market leader', 'growing business',
                'expanding operations', 'multiple departments', 'dedicated team',
                'specialized services', 'full-service', 'comprehensive solutions'
            ]
        }
        
        # Enhanced small business indicators (these get HIGHER scores now)
        self.small_company_indicators = {
            'local_business': [
                'local', 'family owned', 'family business', 'locally owned',
                'community', 'neighborhood', 'hometown', 'serving [city]',
                'local service', 'neighborhood business', 'community focused',
                'locally operated', 'hometown favorite', 'local expertise'
            ],
            'personal_touch': [
                'personal service', 'personalized', 'one-on-one', 'direct contact',
                'owner operated', 'small business', 'boutique', 'specialized',
                'personal attention', 'individual service', 'custom service',
                'tailored solutions', 'hands-on approach', 'direct owner involvement'
            ],
            'simple_structure': [
                'contact us', 'call us', 'email us', 'get in touch', 'reach out',
                'call today', 'contact owner', 'speak directly', 'personal consultation'
            ],
            'authentic_small_business': [
                'family tradition', 'generations', 'local family', 'small team',
                'intimate setting', 'cozy', 'welcoming', 'friendly staff',
                'know your name', 'personal relationships', 'community member',
                'local knowledge', 'neighborhood expert', 'personal touch'
            ],
            'single_location_indicators': [
                'our location', 'visit us at', 'stop by', 'come see us',
                'our shop', 'our store', 'our office', 'single location',
                'one location', 'locally based', 'conveniently located'
            ]
        }
        
        # Technology stack indicators for company size
        self.tech_indicators = {
            'enterprise_tech': [
                'salesforce', 'oracle', 'sap', 'microsoft dynamics', 'workday',
                'servicenow', 'tableau', 'adobe experience', 'marketo', 'hubspot enterprise'
            ],
            'enterprise_hosting': [
                'akamai', 'cloudflare enterprise', 'aws enterprise', 'azure enterprise',
                'google cloud enterprise', 'cdn', 'load balancer', 'redundancy'
            ],
            'small_business_tech': [
                'wix', 'squarespace', 'wordpress.com', 'weebly', 'shopify basic',
                'godaddy website builder', 'site123'
            ]
        }
        
        # Business model classification for vacation rental industry
        self.vacation_rental_business_models = {
            'marketplace_platforms': {
                'keywords': [
                    'airbnb', 'vrbo', 'booking.com', 'expedia', 'tripadvisor rentals',
                    'homeaway', 'vacasa', 'hometogo', 'flipkey', 'vacationrenter',
                    'rentals.com', 'redawning', 'turnkey', 'awaze', 'novasol',
                    'marketplace', 'platform', 'book now', 'search rentals',
                    'find vacation rentals', 'browse properties', 'compare prices',
                    'book direct', 'instant book', 'millions of rentals',
                    'thousands of properties', 'rental marketplace'
                ],
                'url_indicators': [
                    'airbnb.com', 'vrbo.com', 'booking.com', 'expedia.com',
                    'tripadvisor.com', 'homeaway.com', 'vacasa.com', 'hometogo.com'
                ],
                'exclusion_reason': 'marketplace_platform'
            },
            'third_party_listings': {
                'url_patterns': [
                    r'/property/\d+',
                    r'/listing/\d+', 
                    r'/rental/\d+',
                    r'/room/\d+',
                    r'/accommodation/\d+',
                    r'/p/\d+',
                    r'/r/\d+',
                    r'/l/\d+',
                    r'/properties/\d+',
                    r'/rentals/\d+',
                    r'/units?/\d+',
                    r'/homes?/\d+',
                    r'\?.*(?:property|listing|rental)_?id=\d+',
                    r'\?.*id=\d+.*(?:property|listing|rental)',
                    r'/detail/\d+',
                    r'/view/\d+',
                    r'/show/\d+',
                    r'property\..*\.com',
                    r'listing\..*\.com',
                    r'rental\..*\.com'
                ],
                'content_indicators': [
                    'property id', 'listing id', 'rental id', 'property #', 'listing #',
                    'property number', 'listing number', 'id:', 'ref:', 'reference:',
                    'book this property', 'reserve this listing', 'property details',
                    'listing details', 'view more properties', 'browse more rentals',
                    'similar properties', 'more listings like this', 'other properties',
                    'property amenities', 'listing amenities', 'check availability',
                    'booking calendar', 'reservation system', 'instant booking',
                    'property photos', 'listing photos', 'property gallery',
                    'hosted by', 'managed by', 'listed by', 'property owner:',
                    'contact host', 'message host', 'call host', 'property manager',
                    'booking fee', 'service fee', 'cleaning fee', 'security deposit',
                    'cancellation policy', 'house rules', 'guest reviews',
                    'verified listing', 'verified property', 'trust & safety'
                ],
                'template_indicators': [
                    'check-in:', 'check-out:', 'guests:', 'bedrooms:', 'bathrooms:',
                    'sleeps up to', 'accommodates', 'maximum occupancy',
                    'wifi included', 'parking included', 'pet friendly',
                    'smoking policy', 'minimum stay', 'maximum stay',
                    'property type:', 'accommodation type:', 'rental type:',
                    'neighborhood:', 'area:', 'location:', 'address:',
                    'price per night', 'nightly rate', 'weekly rate', 'monthly rate',
                    'total price', 'taxes and fees', 'additional charges'
                ],
                'navigation_indicators': [
                    'browse properties', 'search rentals', 'find accommodation',
                    'property search', 'rental search', 'advanced search',
                    'filter results', 'sort by', 'map view', 'list view',
                    'saved properties', 'favorite listings', 'compare properties',
                    'recently viewed', 'recommended for you', 'popular destinations',
                    'top-rated properties', 'new listings', 'last minute deals'
                ],
                'generic_contact_indicators': [
                    'customer service', 'customer support', 'help center',
                    'support team', 'booking support', 'contact us',
                    'help desk', 'call center', '1-800-', '1-888-', '1-877-',
                    'toll free', 'support@', 'help@', 'booking@', 'reservations@',
                    'info@', 'contact@', 'customer@', 'service@'
                ],
                'exclusion_reason': 'third_party_listing'
            },
            'b2b_service_providers': {
                'keywords': [
                    'property management software', 'vacation rental software', 
                    'channel manager', 'pms', 'property management system',
                    'rental management platform', 'booking engine', 'reservation system',
                    'dynamic pricing', 'revenue management', 'pricing tool',
                    'cleaning management', 'maintenance software', 'guest messaging',
                    'automation tools', 'rental tools', 'property manager tools',
                    'vacation rental management', 'short term rental software',
                    'airbnb management', 'rental automation', 'hospitality software',
                    'directory of tools', 'tools and resources', 'software solutions',
                    'service provider', 'technology partner', 'integration',
                    'api', 'white label', 'enterprise solution', 'saas',
                    'subscription', 'pricing plans', 'free trial', 'demo',
                    'for property managers', 'for hosts', 'for owners'
                ],
                'exclusion_reason': 'b2b_service_provider'
            },
            'marketing_lead_gen': {
                'keywords': [
                    'leads', 'lead generation', 'marketing services', 'seo services',
                    'website design', 'digital marketing', 'social media marketing',
                    'advertising', 'promotion', 'marketing platform', 'generate bookings',
                    'increase revenue', 'boost occupancy', 'marketing tools',
                    'listing optimization', 'rank higher', 'more visibility',
                    'marketing agency', 'consulting', 'growth services'
                ],
                'exclusion_reason': 'marketing_service'
            },
            'aggregator_listing_sites': {
                'keywords': [
                    'compare', 'search all sites', 'aggregator', 'find deals',
                    'best prices', 'price comparison', 'all vacation rentals',
                    'search engine', 'rental search', 'compare rentals',
                    'find rentals', 'rental finder', 'vacation rental search',
                    'browse all', 'search properties', 'rental listings',
                    'property search', 'vacation search'
                ],
                'exclusion_reason': 'aggregator_site'
            },
            'actual_rental_operators': {
                # These are the ones we WANT - actual vacation rental businesses
                'positive_indicators': [
                    'our properties', 'our rentals', 'our vacation homes',
                    'family owned', 'locally owned', 'established', 'since',
                    'years of experience', 'personal service', 'local knowledge',
                    'property portfolio', 'rental portfolio', 'vacation homes',
                    'beach houses', 'mountain cabins', 'lake houses',
                    'contact us', 'call us', 'email us', 'visit us',
                    'based in', 'located in', 'serving', 'specializing in',
                    'luxury rentals', 'premium properties', 'exclusive rentals',
                    'hand-picked', 'carefully selected', 'curated collection',
                    'direct owner', 'property owner', 'private owner',
                    'no booking fees', 'no service fees', 'book direct',
                    'personal attention', 'concierge service', 'local host'
                ],
                'business_model': 'direct_rental_operator'
            }
        }
        
        # Industry classification keywords
        self.industry_keywords = {
            'vacation_rental': [
                'vacation rental', 'holiday rental', 'vacation home', 'holiday home',
                'short term rental', 'vacation property', 'rental property',
                'beach house', 'cabin rental', 'cottage rental', 'villa rental',
                'vacation accommodation', 'holiday accommodation', 'getaway',
                'book now', 'check availability', 'nightly rate', 'per night',
                'airbnb', 'vrbo', 'homeaway', 'booking.com', 'expedia',
                'resort', 'lodge', 'retreat', 'bed and breakfast', 'b&b',
                'oceanfront', 'beachfront', 'lakefront', 'mountain view',
                'private pool', 'hot tub', 'fireplace', 'kitchen', 'wifi'
            ],
            'real_estate': [
                'real estate', 'property', 'homes for sale', 'houses for sale',
                'buy home', 'sell home', 'realtor', 'real estate agent',
                'mls', 'multiple listing', 'property search', 'home search',
                'mortgage', 'loan', 'financing', 'closing', 'escrow',
                'square feet', 'sq ft', 'bedroom', 'bathroom', 'garage',
                'lot size', 'acre', 'price reduced', 'new listing', 'sold'
            ],
            'dental': [
                'dentist', 'dental', 'teeth', 'tooth', 'oral health',
                'dental care', 'dental office', 'dental practice', 'orthodontist',
                'oral surgeon', 'periodontist', 'endodontist', 'hygienist',
                'cleaning', 'filling', 'crown', 'bridge', 'implant',
                'whitening', 'braces', 'invisalign', 'root canal', 'extraction'
            ],
            'home_services': [
                'plumber', 'plumbing', 'electrician', 'electrical', 'hvac',
                'heating', 'cooling', 'air conditioning', 'contractor',
                'construction', 'renovation', 'remodeling', 'roofing',
                'painting', 'flooring', 'landscaping', 'cleaning service',
                'handyman', 'repair', 'installation', 'maintenance'
            ],
            'legal': [
                'lawyer', 'attorney', 'legal', 'law firm', 'legal services',
                'personal injury', 'divorce', 'criminal defense', 'bankruptcy',
                'estate planning', 'wills', 'probate', 'litigation',
                'consultation', 'legal advice', 'court', 'settlement'
            ],
            'financial': [
                'bank', 'banking', 'credit union', 'financial', 'loan',
                'mortgage', 'insurance', 'investment', 'financial advisor',
                'accounting', 'tax', 'cpa', 'bookkeeping', 'payroll',
                'retirement', '401k', 'ira', 'wealth management', 'portfolio'
            ],
            'restaurant_food': [
                'restaurant', 'cafe', 'bar', 'dining', 'menu', 'food',
                'catering', 'delivery', 'takeout', 'reservation', 'chef',
                'cuisine', 'breakfast', 'lunch', 'dinner', 'pizza',
                'burger', 'coffee', 'bakery', 'deli', 'grill'
            ],
            'healthcare_medical': [
                'doctor', 'physician', 'medical', 'clinic', 'hospital',
                'health', 'patient', 'appointment', 'treatment', 'surgery',
                'therapy', 'diagnosis', 'prescription', 'insurance',
                'medicare', 'medicaid', 'emergency', 'urgent care'
            ]
        }
        
        # NEW: Enhanced vacation rental business model classification
        self.enhanced_vr_business_models = {
            'direct_owner_small': {
                'keywords': [
                    'our property', 'our home', 'our cabin', 'our cottage', 'our villa',
                    'my property', 'my home', 'my cabin', 'family owned', 'family vacation home',
                    'personal vacation', 'private owner', 'owner direct', 'by owner',
                    'no booking fees', 'book direct and save', 'family retreat',
                    'our beach house', 'our mountain home', 'our lake house',
                    'we purchased', 'we bought', 'we renovated', 'we built'
                ],
                'property_count_indicators': ['property', 'home', 'house', 'cabin', 'cottage'],
                'property_range': (1, 5),
                'is_target': True,
                'priority': 'high'
            },
            'direct_owner_medium': {
                'keywords': [
                    'our properties', 'our homes', 'our rentals', 'portfolio',
                    'collection of homes', 'several properties', 'multiple locations',
                    'expanding our', 'growing portfolio', 'newest addition',
                    'properties include', 'locations include', 'we own',
                    'investment properties', 'rental portfolio'
                ],
                'property_count_indicators': ['properties', 'homes', 'rentals', 'units'],
                'property_range': (6, 15),
                'is_target': True,
                'priority': 'high'
            },
            'property_manager_small': {
                'keywords': [
                    'we manage', 'professionally managed', 'management services',
                    'local property management', 'property care', 'full service management',
                    'locally owned and operated', 'boutique property management',
                    'personalized management', 'hands-on management', 'dedicated team',
                    'small company', 'local company', 'family business',
                    'select properties', 'curated collection', 'hand-picked homes'
                ],
                'property_count_indicators': ['manage', 'managing', 'portfolio', 'properties under management'],
                'property_range': (10, 50),
                'is_target': True,
                'priority': 'high'
            },
            'property_manager_medium': {
                'keywords': [
                    'professional property management', 'established management company',
                    'regional leader', 'growing management', 'multiple office locations',
                    'expanding portfolio', 'acquisitions', 'new markets',
                    'management team', 'property managers', 'experienced staff',
                    'comprehensive management', 'full-service company'
                ],
                'property_count_indicators': ['properties', 'units', 'rentals', 'homes under management'],
                'property_range': (51, 200),
                'is_target': True,
                'priority': 'medium'
            },
            'property_manager_large': {
                'keywords': [
                    'nationwide', 'multiple states', 'corporate', 'enterprise',
                    'leading property management', 'largest', 'hundreds of properties',
                    'institutional', 'corporate housing', 'investor relations',
                    'publicly traded', 'franchise', 'multi-state operations'
                ],
                'property_count_indicators': ['properties', 'units', 'locations'],
                'property_range': (201, 10000),
                'is_target': False,
                'priority': 'exclude',
                'exclusion_reason': 'too_large'
            },
            'listing_platform_small': {
                'keywords': [
                    'local directory', 'area listings', 'regional marketplace',
                    'find rentals in', 'search properties', 'browse homes',
                    'list your property', 'add your rental', 'featured listings',
                    'local vacation rentals', 'area vacation homes'
                ],
                'url_patterns': ['/search', '/listings', '/browse', '/find'],
                'is_target': True,  # They might want to upgrade to a booking platform
                'priority': 'medium'
            },
            'listing_platform_large': {
                'keywords': [
                    'airbnb', 'vrbo', 'homeaway', 'booking.com', 'expedia',
                    'thousands of properties', 'millions of listings', 'global marketplace',
                    'book your next', 'compare prices', 'instant booking',
                    'traveler reviews', 'verified properties', 'secure booking'
                ],
                'is_target': False,
                'priority': 'exclude',
                'exclusion_reason': 'marketplace_platform'
            },
            'software_provider': {
                'keywords': [
                    'property management software', 'pms', 'saas', 'booking engine',
                    'channel manager', 'vacation rental software', 'automation',
                    'api', 'integration', 'white label', 'pricing plans',
                    'free trial', 'demo', 'features', 'solutions for',
                    'tools for property managers', 'software for', 'platform for'
                ],
                'is_target': False,
                'priority': 'exclude',
                'exclusion_reason': 'b2b_software'
            },
            'marketing_agency': {
                'keywords': [
                    'marketing services', 'seo for vacation rentals', 'listing optimization',
                    'increase bookings', 'digital marketing', 'social media management',
                    'content creation', 'photography services', 'virtual tours',
                    'lead generation', 'advertising services', 'promotion'
                ],
                'is_target': False,
                'priority': 'exclude',
                'exclusion_reason': 'marketing_service'
            }
        }
        
        # NEW: Decision maker accessibility indicators
        self.decision_maker_indicators = {
            'high_accessibility': {
                'patterns': [
                    r'\b(call|contact|text)\s+\w+\s+(directly|personally)',
                    r'owner[\s\-]?(direct|operated)',
                    r'ask for \w+',
                    r'speak (to|with) \w+',
                    r'\w+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}',  # Personal email (firstname@domain)
                    r'my (name is|cell|phone|direct)',
                    r'I (started|founded|own|manage)',
                    r'(family|locally) owned and operated'
                ],
                'keywords': [
                    'owner direct', 'call me', 'text me', 'personally manage',
                    'family owned', 'owner operated', 'ask for', 'speak to',
                    'direct line', 'cell phone', 'mobile number', 'personal service'
                ],
                'negative_keywords': [
                    'corporate', 'headquarters', 'investor relations', 'press inquiries',
                    'human resources', 'legal department', 'compliance'
                ]
            },
            'low_accessibility': {
                'keywords': [
                    'corporate office', 'headquarters', 'investor relations',
                    'press inquiries', 'media contact', 'hr department',
                    'legal counsel', 'board of directors', 'shareholders',
                    'publicly traded', 'stock symbol', 'sec filings'
                ],
                'email_patterns': ['info@', 'contact@', 'support@', 'hello@', 'admin@']
            }
        }
        
        # NEW: Website quality and upgrade need indicators
        self.website_quality_indicators = {
            'needs_upgrade': {
                'technical': [
                    'last updated', 'under construction', 'coming soon',
                    'best viewed in internet explorer', 'requires flash',
                    'site best viewed at', 'enable javascript', 'frames version',
                    'text only version', 'low bandwidth version'
                ],
                'functional': [
                    'email for availability', 'call for rates', 'contact for pricing',
                    'inquire about dates', 'check availability by phone',
                    'no online booking', 'email to reserve', 'call to book',
                    'fax your request', 'mail check', 'money order accepted'
                ],
                'design': [
                    'welcome to my website', 'thanks for visiting', 'you are visitor number',
                    'page counter', 'guestbook', 'sign my guestbook', 'web ring',
                    'site map', 'frames', 'table layout', 'animated gif',
                    'under construction gif', 'spinning logo', 'marquee text'
                ],
                'age_patterns': [
                    r'copyright\s*(?:©)?\s*(\d{4})',  # Old copyright years
                    r'last\s*updated?\s*:?\s*(\d{4})',
                    r'(?:established|since|founded)\s*:?\s*(\d{4})'
                ]
            },
            'modern_indicators': {
                'technical': [
                    'mobile responsive', 'ssl secure', 'instant booking',
                    'real-time availability', 'online payment', 'secure checkout',
                    'api integration', 'channel sync', 'dynamic pricing'
                ],
                'platforms': [
                    'wordpress 5', 'react', 'vue', 'angular', 'next.js',
                    'gatsby', 'jamstack', 'headless cms', 'graphql'
                ]
            }
        }
        
        # NEW: Property count detection patterns
        self.property_count_patterns = [
            # Specific numbers
            (r'(\d+)\s*(?:properties|homes|rentals|units|cabins|cottages|villas|condos)', 'exact'),
            (r'(?:manage|managing|own|offer)\s*(\d+)', 'exact'),
            (r'portfolio of\s*(\d+)', 'exact'),
            (r'(\d+)\s*vacation\s*(?:homes|properties|rentals)', 'exact'),
            
            # Ranges
            (r'(\d+)\s*(?:to|-)\s*(\d+)\s*(?:properties|homes|rentals)', 'range'),
            (r'between\s*(\d+)\s*and\s*(\d+)', 'range'),
            
            # Descriptive
            (r'dozens of properties', 'dozens'),
            (r'hundreds of properties', 'hundreds'),
            (r'thousands of properties', 'thousands'),
            (r'handful of properties', 'handful'),
            (r'few select properties', 'few'),
            (r'multiple properties', 'multiple'),
            (r'several properties', 'several')
        ]
        
        # NEW: Geographic scope patterns
        self.geographic_scope_patterns = {
            'local': {
                'keywords': ['local', 'locally owned', 'serving [city]', 'in [city]', 
                           'area', 'neighborhood', 'community', 'hometown'],
                'patterns': [r'serving\s+\w+\s*(?:area|region|community)'],
                'scope_score': 1
            },
            'regional': {
                'keywords': ['regional', 'multiple cities', 'throughout [state]', 
                           'across [state]', 'statewide', '[state] properties'],
                'patterns': [r'(?:throughout|across)\s+\w+'],
                'scope_score': 2
            },
            'national': {
                'keywords': ['nationwide', 'national', 'coast to coast', 
                           'multiple states', 'across america', 'usa wide'],
                'patterns': [r'(?:nationwide|national|multiple states)'],
                'scope_score': 3
            },
            'international': {
                'keywords': ['international', 'global', 'worldwide', 
                           'multiple countries', 'globally'],
                'patterns': [r'(?:international|global|worldwide)'],
                'scope_score': 4
            }
        }
        
        # NEW: Industry-specific patterns for vacation rentals
        self.vr_industry_patterns = {
            'beach': {
                'keywords': ['beach', 'ocean', 'oceanfront', 'beachfront', 'coastal',
                           'seaside', 'shore', 'gulf', 'atlantic', 'pacific',
                           'sand', 'surf', 'waves', 'tides', 'dunes'],
                'seasonal_indicators': ['summer rentals', 'spring break', 'off-season rates'],
                'amenity_keywords': ['beach access', 'ocean view', 'beach chairs', 'umbrellas']
            },
            'mountain': {
                'keywords': ['mountain', 'ski', 'alpine', 'cabin', 'chalet',
                           'elevation', 'peaks', 'slopes', 'trails', 'hiking',
                           'ski-in', 'ski-out', 'lodge', 'retreat'],
                'seasonal_indicators': ['ski season', 'summer hiking', 'fall colors'],
                'amenity_keywords': ['hot tub', 'fireplace', 'ski storage', '4wd required']
            },
            'lake': {
                'keywords': ['lake', 'lakefront', 'waterfront', 'dock', 'boat',
                           'fishing', 'swimming', 'water sports', 'marina'],
                'seasonal_indicators': ['summer season', 'boating season'],
                'amenity_keywords': ['private dock', 'boat launch', 'fishing gear', 'kayaks']
            },
            'urban': {
                'keywords': ['downtown', 'city center', 'metro', 'urban', 'walkable',
                           'transit', 'nightlife', 'restaurants', 'shopping'],
                'seasonal_indicators': ['event pricing', 'convention rates'],
                'amenity_keywords': ['parking included', 'walk to', 'public transit', 'wifi']
            },
            'rural': {
                'keywords': ['country', 'rural', 'farm', 'ranch', 'secluded',
                           'private', 'acres', 'peaceful', 'quiet', 'nature'],
                'seasonal_indicators': ['harvest season', 'hunting season'],
                'amenity_keywords': ['acreage', 'privacy', 'wildlife', 'stargazing']
            }
        }

    # NEW METHODS - Add these after existing methods

    def detect_property_count(self, text):
        """Detect number of properties mentioned"""
        try:
            text_lower = text.lower()
            
            # Check exact number patterns
            for pattern, pattern_type in self.property_count_patterns:
                if pattern_type == 'exact':
                    matches = re.findall(pattern, text_lower)
                    for match in matches:
                        try:
                            count = int(match)
                            if 1 <= count <= 10000:  # Reasonable range
                                return {'count': count, 'confidence': 90, 'type': 'exact'}
                        except:
                            continue
                
                elif pattern_type == 'range':
                    matches = re.findall(pattern, text_lower)
                    for match in matches:
                        try:
                            if isinstance(match, tuple):
                                low, high = int(match[0]), int(match[1])
                                avg = (low + high) // 2
                                return {'count': avg, 'confidence': 80, 'type': 'range'}
                        except:
                            continue
                
                else:
                    # Descriptive patterns
                    if re.search(pattern, text_lower):
                        estimates = {
                            'handful': 3, 'few': 5, 'several': 8, 'multiple': 12,
                            'dozens': 36, 'hundreds': 200, 'thousands': 2000
                        }
                        return {
                            'count': estimates.get(pattern_type, 10),
                            'confidence': 60,
                            'type': 'estimate'
                        }
            
            return {'count': None, 'confidence': 0, 'type': None}
            
        except Exception as e:
            logger.error(f"Error detecting property count: {e}")
            return {'count': None, 'confidence': 0, 'type': None}

    def calculate_decision_maker_score(self, soup, text, business_info):
        """Calculate how accessible the decision maker is"""
        try:
            score = 0
            indicators_found = []
            
            # Check for high accessibility patterns
            for pattern in self.decision_maker_indicators['high_accessibility']['patterns']:
                if re.search(pattern, text, re.IGNORECASE):
                    score += 20
                    indicators_found.append(f"Pattern: {pattern}")
            
            # Check for high accessibility keywords
            for keyword in self.decision_maker_indicators['high_accessibility']['keywords']:
                if keyword.lower() in text.lower():
                    score += 10
                    indicators_found.append(f"Keyword: {keyword}")
            
            # Check email patterns
            emails = business_info.get('emails', [])
            for email in emails:
                # Personal email (firstname@domain) scores high
                if email and not any(prefix in email.lower() for prefix in 
                                    self.decision_maker_indicators['low_accessibility']['email_patterns']):
                    score += 25
                    indicators_found.append(f"Personal email: {email}")
            
            # Check for negative indicators
            for keyword in self.decision_maker_indicators['high_accessibility']['negative_keywords']:
                if keyword.lower() in text.lower():
                    score -= 15
                    indicators_found.append(f"Negative: {keyword}")
            
            # Check for low accessibility indicators
            for keyword in self.decision_maker_indicators['low_accessibility']['keywords']:
                if keyword.lower() in text.lower():
                    score -= 20
                    indicators_found.append(f"Corporate indicator: {keyword}")
            
            # Phone number accessibility
            phones = business_info.get('phones', [])
            if phones:
                # Check if it's a personal/cell phone pattern
                for phone in phones:
                    if re.search(r'(cell|mobile|direct|personal)', text.lower()):
                        score += 15
                        indicators_found.append("Personal phone number")
            
            # Website size factor (smaller sites = more accessible)
            website_metrics = business_info.get('website_metrics', {})
            if website_metrics.get('word_count', 0) < 1000:
                score += 10
                indicators_found.append("Small website (likely owner-operated)")
            
            # Calculate confidence
            confidence = min(95, abs(score))
            accessibility_level = 'high' if score > 30 else 'medium' if score > 0 else 'low'
            
            return {
                'score': score,
                'confidence': confidence,
                'level': accessibility_level,
                'indicators': indicators_found
            }
            
        except Exception as e:
            logger.error(f"Error calculating decision maker score: {e}")
            return {'score': 0, 'confidence': 0, 'level': 'unknown', 'indicators': []}

    def detect_website_upgrade_needs(self, soup, text, response):
        """Detect if website needs upgrading"""
        try:
            upgrade_score = 0
            upgrade_indicators = []
            modern_score = 0
            
            # Check technical indicators
            for indicator in self.website_quality_indicators['needs_upgrade']['technical']:
                if indicator.lower() in text.lower():
                    upgrade_score += 15
                    upgrade_indicators.append(f"Technical issue: {indicator}")
            
            # Check functional limitations
            for indicator in self.website_quality_indicators['needs_upgrade']['functional']:
                if indicator.lower() in text.lower():
                    upgrade_score += 20
                    upgrade_indicators.append(f"Functional limitation: {indicator}")
            
            # Check design issues
            for indicator in self.website_quality_indicators['needs_upgrade']['design']:
                if indicator.lower() in text.lower():
                    upgrade_score += 10
                    upgrade_indicators.append(f"Design issue: {indicator}")
            
            # Check copyright/update dates
            for pattern in self.website_quality_indicators['needs_upgrade']['age_patterns']:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    try:
                        year = int(match)
                        if year < 2020:  # Site older than 4 years
                            age_score = (2024 - year) * 5
                            upgrade_score += age_score
                            upgrade_indicators.append(f"Old date found: {year}")
                    except:
                        continue
            
            # Check for modern indicators (reduces upgrade need)
            for indicator in self.website_quality_indicators['modern_indicators']['technical']:
                if indicator.lower() in text.lower():
                    modern_score += 15

             # Bonus points for poor quality sites (they need help!)
            word_count = len(text.split())
            if word_count < 500:
                upgrade_score += 30
                upgrade_indicators.append("Very small website - needs content")
            
            if not soup.find(['form', 'button']) or 'contact' not in text.lower():
                upgrade_score += 25
                upgrade_indicators.append("No contact form or booking system")       
            
            # Check SSL
            if response and response.url.startswith('https'):
                modern_score += 10
            else:
                upgrade_score += 20
                upgrade_indicators.append("No SSL certificate")
            
            # Check mobile responsiveness
            viewport_tag = soup.find('meta', attrs={'name': 'viewport'})
            if not viewport_tag:
                upgrade_score += 25
                upgrade_indicators.append("Not mobile responsive")
            
            # Check for outdated technology
            if soup.find_all('frame') or soup.find_all('frameset'):
                upgrade_score += 30
                upgrade_indicators.append("Uses frames (very outdated)")
            
            # Table-based layout detection
            tables = soup.find_all('table')
            if len(tables) > 5:
                # Check if tables are used for layout
                layout_tables = [t for t in tables if not t.find_all('th')]
                if len(layout_tables) > 3:
                    upgrade_score += 20
                    upgrade_indicators.append("Table-based layout")
            
            # Calculate final score
            final_score = max(0, upgrade_score - modern_score)
            needs_upgrade = final_score > 40
            
            return {
                'needs_upgrade': needs_upgrade,
                'upgrade_score': final_score,
                'indicators': upgrade_indicators,
                'confidence': min(95, final_score)
            }
            
        except Exception as e:
            logger.error(f"Error detecting website upgrade needs: {e}")
            return {
                'needs_upgrade': False,
                'upgrade_score': 0,
                'indicators': [],
                'confidence': 0
            }

    def classify_vr_property_type(self, text, title, description):
        """Classify what type of vacation rental property this is"""
        try:
            property_scores = {}
            
            all_text = (text + ' ' + (title or '') + ' ' + (description or '')).lower()
            
            for prop_type, patterns in self.vr_industry_patterns.items():
                score = 0
                
                # Check main keywords
                for keyword in patterns['keywords']:
                    count = all_text.count(keyword)
                    score += count * 3
                
                # Check seasonal indicators
                for indicator in patterns['seasonal_indicators']:
                    if indicator in all_text:
                        score += 5
                
                # Check amenity keywords
                for amenity in patterns['amenity_keywords']:
                    if amenity in all_text:
                        score += 4
                
                if score > 0:
                    property_scores[prop_type] = score
            
            if not property_scores:
                return {'type': 'general', 'confidence': 0}
            
            # Get the highest scoring type
            top_type = max(property_scores, key=property_scores.get)
            confidence = min(95, property_scores[top_type] * 2)
            
            return {
                'type': top_type,
                'confidence': confidence,
                'scores': property_scores
            }
            
        except Exception as e:
            logger.error(f"Error classifying VR property type: {e}")
            return {'type': 'unknown', 'confidence': 0}

    # 2. FIX: Enhanced vacation rental classification to better detect listing sites
    def enhanced_classify_vacation_rental_business(self, soup, page_text, title, description, final_url, business_info):
        """Enhanced classification focusing on CONTENT, not domain names"""
        try:
            # Get additional content from other pages
            additional_content = ''
            if self.enable_deep_crawl and final_url:  # <-- ADD THIS LINE HERE
            additional_content = self.crawl_additional_pages(final_url, soup)  # <-- AND THIS LINE HERE
            logger.info(f"Crawled {len(additional_content.split())} additional words from other pages")
        
            # Combine all content
            all_text = (page_text + ' ' + (title or '') + ' ' + (description or '') + ' ' + additional_content).lower()
        
            # Initialize tracking
            model_scores = {}
            property_count_info = self.detect_property_count(all_text)
            decision_maker_score = self.calculate_decision_maker_score(soup, page_text, business_info)
            website_upgrade_info = self.detect_website_upgrade_needs(soup, page_text, None)
            property_type_info = self.classify_vr_property_type(page_text, title, description)
            
            # REMOVED: Domain-based classification
            # Now we ONLY look at content
            

            # Optionally crawl additional pages for vacation rental sites
            additional_content = ''
            if result.get('industry_type') == 'vacation_rental' or 'vacation' in page_text.lower():
                additional_content = self.crawl_additional_pages(result.get('final_url', ''), soup)
            
            # Use combined content for classification
            combined_text = page_text + ' ' + additional_content
            
            # 1. DEEP CONTENT ANALYSIS - Look for actual rental operator indicators
            rental_operator_score = 0
            rental_operator_indicators = []
            
            # Strong indicators they actually rent properties
            actual_rental_phrases = [
                # Ownership indicators
                'our vacation rental', 'our property', 'our home', 'our beach house',
                'our cabin', 'our cottage', 'we own', 'property we manage',
                'welcome to our', 'stay at our', 'rent our', 'book our',
                
                # Direct booking language
                'book directly with us', 'book direct', 'no booking fees',
                'contact us directly', 'call us to book', 'email for rates',
                'check our calendar', 'see availability', 'reserve now',
                
                # Property descriptions
                'sleeps', 'bedrooms', 'bathrooms', 'square feet', 'accommodates',
                'fully equipped kitchen', 'private pool', 'ocean view', 'mountain view',
                'walking distance', 'minutes from', 'located in', 'situated on',
                
                # Amenities lists
                'amenities include', 'features include', 'property features',
                'what we offer', 'included in your stay', 'guest access',
                
                # Rates and policies
                'nightly rate', 'weekly rate', 'seasonal rates', 'minimum stay',
                'cleaning fee', 'security deposit', 'cancellation policy',
                'house rules', 'check-in time', 'check-out time',
                
                # Local host indicators
                'your host', 'meet your host', 'about us', 'why choose us',
                'local recommendations', 'area guide', 'things to do',
                'we recommend', 'our favorite', 'local tips'
            ]
            
            for phrase in actual_rental_phrases:
                if phrase in all_text:
                    rental_operator_score += 10
                    rental_operator_indicators.append(phrase)
            
            # 2. CHECK FOR LISTING PLATFORM INDICATORS (content-based)
            listing_platform_score = 0
            listing_indicators = []
            
            listing_platform_phrases = [
                # Search functionality
                'search properties', 'find rentals', 'browse listings',
                'filter results', 'sort by price', 'map view',
                'search by location', 'advanced search', 'refine search',
                
                # Multiple properties language
                'thousands of properties', 'hundreds of rentals', 
                'properties worldwide', 'rentals in multiple',
                'compare properties', 'similar listings',
                
                # Platform features
                'list your property', 'become a host', 'host dashboard',
                'traveler reviews', 'verified properties', 'trust and safety',
                'secure payments', 'booking protection', '24/7 support',
                
                # Aggregator language
                'best prices guaranteed', 'price match', 'deals from',
                'compare rates', 'lowest prices', 'exclusive deals'
            ]
            
            for phrase in listing_platform_phrases:
                if phrase in all_text:
                    listing_platform_score += 15
                    listing_indicators.append(phrase)
            
            # 3. ANALYZE PAGE STRUCTURE for actual rental content
            content_structure_score = self.analyze_rental_content_structure(soup, all_text)
            
            # 4. CHECK FOR SPECIFIC PROPERTY DETAILS
            has_specific_property = self.detect_specific_property_details(soup, all_text)
            
            # 5. LOOK FOR BOOKING/INQUIRY FORMS
            has_direct_booking = self.detect_direct_booking_capability(soup)
            
            # Calculate model scores based on CONTENT ONLY
            if rental_operator_score > 30 and has_specific_property:
                if property_count_info['count'] and property_count_info['count'] <= 5:
                    model_scores['direct_owner_small'] = rental_operator_score + 50
                elif property_count_info['count'] and property_count_info['count'] <= 15:
                    model_scores['direct_owner_medium'] = rental_operator_score + 40
                else:
                    model_scores['property_manager_small'] = rental_operator_score + 40
            
            if listing_platform_score > 40:
                model_scores['listing_platform_large'] = listing_platform_score
            
            # ... rest of classification logic based on content ...
            
        except Exception as e:
            logger.error(f"Error in content-focused classification: {e}")
            return {
                'business_model': 'unknown',
                'is_target_customer': False,
                'priority': 'low',
                'target_score': 0,
                'exclusion_reason': 'classification_error'
            }

    def analyze_rental_content_structure(self, soup, text):
        """Analyze page structure for vacation rental content patterns"""
        score = 0
        
        # Look for property detail sections
        property_sections = soup.find_all(['div', 'section'], class_=re.compile(
            r'property|rental|accommodation|listing|details|features|amenities', re.I
        ))
        score += len(property_sections) * 5
        
        # Look for rate/pricing tables
        rate_elements = soup.find_all(['table', 'div'], class_=re.compile(r'rate|price|tariff', re.I))
        if rate_elements:
            score += 15
        
        # Look for availability calendars
        calendar_elements = soup.find_all(['div', 'table', 'iframe'], class_=re.compile(
            r'calendar|availability|booking|schedule', re.I
        ))
        if calendar_elements:
            score += 20
        
        # Look for photo galleries
        gallery_elements = soup.find_all(['div', 'section'], class_=re.compile(
            r'gallery|photos|images|slideshow', re.I
        ))
        if gallery_elements:
            # Check if it's property photos vs stock photos
            img_alts = [img.get('alt', '').lower() for img in soup.find_all('img')]
            property_photo_keywords = ['bedroom', 'kitchen', 'living', 'bathroom', 'view', 'pool', 'exterior']
            property_photos = sum(1 for alt in img_alts if any(keyword in alt for keyword in property_photo_keywords))
            if property_photos > 3:
                score += 15
        
        return score

    def detect_specific_property_details(self, soup, text):
        """Check if the page describes a specific property (not a directory)"""
        
        # Count specific property details
        detail_count = 0
        
        # Check for specific number of bedrooms/bathrooms
        bedroom_match = re.search(r'\b(\d+)\s*(?:bed|br|bedroom)', text)
        bathroom_match = re.search(r'\b(\d+)\s*(?:bath|ba|bathroom)', text)
        if bedroom_match and bathroom_match:
            detail_count += 2
        
        # Check for specific address or location
        address_patterns = [
            r'\d+\s+\w+\s+(?:street|st|avenue|ave|road|rd|drive|dr)',
            r'located at\s+[\w\s,]+',
            r'address:\s*[\w\s,]+',
        ]
        for pattern in address_patterns:
            if re.search(pattern, text, re.I):
                detail_count += 1
                break
        
        # Check for specific amenity lists
        if 'amenities:' in text or 'features:' in text or 'includes:' in text:
            detail_count += 1
        
        # Check for property-specific description
        description_keywords = ['spacious', 'cozy', 'renovated', 'modern', 'charming', 
                              'comfortable', 'private', 'peaceful', 'stunning views']
        description_count = sum(1 for keyword in description_keywords if keyword in text)
        if description_count >= 3:
            detail_count += 1
        
        return detail_count >= 3  # Need at least 3 specific details

    def detect_direct_booking_capability(self, soup):
        """Check if site has direct booking/inquiry capability"""
        
        # Look for inquiry/booking forms
        forms = soup.find_all('form')
        for form in forms:
            form_text = form.get_text().lower()
            form_inputs = form.find_all(['input', 'textarea', 'select'])
            
            # Check if it's a booking/inquiry form
            booking_keywords = ['inquiry', 'booking', 'reservation', 'check-in', 
                              'check-out', 'guests', 'dates', 'availability']
            if any(keyword in form_text for keyword in booking_keywords):
                return True
            
            # Check form field names
            for input_field in form_inputs:
                field_name = (input_field.get('name', '') + input_field.get('id', '')).lower()
                if any(keyword in field_name for keyword in booking_keywords):
                    return True
        
        # Look for email/phone with booking context
        booking_context = ['book', 'reserve', 'inquiry', 'availability', 'rates']
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        
        for context in booking_context:
            if context in text and re.search(email_pattern, text):
                return True
        
        return False        
    

    def detect_geographic_scope(self, text):
        """Detect geographic scope of business"""
        try:
            scope_scores = {}
            
            for scope, data in self.geographic_scope_patterns.items():
                score = 0
                
                # Check keywords
                for keyword in data['keywords']:
                    if keyword in text:
                        score += 10
                
                # Check patterns
                for pattern in data['patterns']:
                    if re.search(pattern, text, re.IGNORECASE):
                        score += 15
                
                if score > 0:
                    scope_scores[scope] = score
            
            if not scope_scores:
                return {'scope': 'local', 'confidence': 50}  # Default to local
            
            best_scope = max(scope_scores, key=scope_scores.get)
            confidence = min(95, scope_scores[best_scope] * 3)
            
            return {
                'scope': best_scope,
                'confidence': confidence,
                'scope_score': self.geographic_scope_patterns[best_scope]['scope_score']
            }
            
        except Exception as e:
            logger.error(f"Error detecting geographic scope: {e}")
            return {'scope': 'unknown', 'confidence': 0, 'scope_score': 0}

    # ALL ORIGINAL METHODS REMAIN UNCHANGED BELOW THIS POINT

    def setup_progress_tracking(self, output_dir):
        """Setup progress tracking file"""
        self.progress_file = os.path.join(output_dir, 'progress.json')
        
        # Create initial progress file if it doesn't exist
        if not os.path.exists(self.progress_file):
            initial_progress = {
                'processed_domains': [],
                'current_batch': 0,
                'total_batches': 0,
                'start_time': None,
                'last_update': None,
                'stats': {
                    'total_processed': 0,
                    'working': 0,
                    'business': 0,
                    'parked': 0,
                    'failed': 0,
                    'excluded_businesses': {},
                }
            }
            self.save_progress(initial_progress)
        
        logger.info(f"Progress tracking file: {self.progress_file}")

    def load_progress(self):
        """Load progress from file"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    self.processed_domains = set(progress.get('processed_domains', []))
                    self.current_batch = progress.get('current_batch', 0)
                    self.total_batches = progress.get('total_batches', 0)
                    saved_stats = progress.get('stats', {})
                    
                    # Update stats
                    for key, value in saved_stats.items():
                        if key in self.stats:
                            self.stats[key] = value
                    
                    logger.info(f"Loaded progress: {len(self.processed_domains)} domains processed")
                    return True
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
        return False

    def save_progress(self, progress_data=None):
        """Save progress to file"""
        try:
            if progress_data is None:
                progress_data = {
                    'processed_domains': list(self.processed_domains),
                    'current_batch': self.current_batch,
                    'total_batches': self.total_batches,
                    'start_time': self.stats.get('start_time'),
                    'last_update': datetime.now().isoformat(),
                    'stats': self.stats
                }
            
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    def check_network_connectivity(self):
        """Check if we have basic internet connectivity with enhanced monitoring"""
        with self.connectivity_lock:
            current_time = time.time()
            
            # Check connectivity every 30 seconds or after consecutive failures
            if (current_time - self.last_connectivity_check < 30 and 
                self.consecutive_failures < 3):
                return True
            
            self.last_connectivity_check = current_time
            
            test_urls = [
                'https://8.8.8.8',
                'https://google.com',
                'https://cloudflare.com',
                'https://github.com'
            ]
            
            successful_connections = 0
            for url in test_urls:
                try:
                    response = self.session.get(url, timeout=5, allow_redirects=False)
                    if response.status_code in [200, 301, 302, 403, 404]:
                        successful_connections += 1
                except Exception as e:
                    logger.debug(f"Connectivity test failed for {url}: {e}")
                    continue
            
            # Need at least 2 successful connections
            if successful_connections >= 2:
                self.network_issues_count = 0
                self.consecutive_failures = 0
                logger.info("✅ Network connectivity confirmed")
                return True
            else:
                self.network_issues_count += 1
                self.consecutive_failures += 1
                logger.warning(f"⚠️ Network connectivity issues detected (attempt {self.consecutive_failures})")
                
                # Wait before retrying if we have connectivity issues
                if self.consecutive_failures >= 3:
                    logger.warning("🔄 Waiting 30 seconds before retry due to connectivity issues...")
                    time.sleep(30)
                
                return False

    def wait_for_connectivity(self, max_retries=5):
        """Wait for network connectivity to be restored"""
        for attempt in range(max_retries):
            if self.check_network_connectivity():
                return True
            
            wait_time = min(60 * (2 ** attempt), 300)  # Exponential backoff, max 5 minutes
            logger.warning(f"❌ No connectivity. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
            time.sleep(wait_time)
        
        return False

    def check_domain(self, domain):
        """Check a single domain for availability and business information"""
        # Check connectivity before processing
        if not self.check_network_connectivity():
            if not self.wait_for_connectivity():
                return {
                    'domain': domain,
                    'working': False,
                    'final_url': '',
                    'protocol': '',
                    'status_code': '',
                    'title': '',
                    'description': '',
                    'is_parked': False,
                    'is_business': False,
                    'business_info': {},
                    'industry_type': '',
                    'industry_confidence': 0,
                    'company_size': '',
                    'size_confidence': 0,
                    'size_details': {},
                    'vr_business_model': '',
                    'vr_exclusion_reason': '',
                    'is_target_customer': '',
                    'vr_model_confidence': 0,
                    'error': 'No network connectivity',
                    'failed_due_to_connectivity': True
                }
        
        result = {
            'domain': domain,
            'working': False,
            'final_url': '',
            'protocol': '',
            'status_code': '',
            'title': '',
            'description': '',
            'is_parked': False,
            'is_business': False,
            'business_info': {},
            'industry_type': '',
            'industry_confidence': 0,
            'company_size': '',
            'size_confidence': 0,
            'size_details': {},
            'vr_business_model': '',
            'vr_exclusion_reason': '',
            'is_target_customer': '',
            'vr_model_confidence': 0,
            'error': '',
            'failed_due_to_connectivity': False
        }
        
        domain = domain.strip().lower()
        if domain.startswith('http'):
            domain = urlparse(domain).netloc
        
        for protocol in ['https', 'http']:
            url = f"{protocol}://{domain}"
            try:
                response = self.session.get(url, timeout=self.timeout, allow_redirects=True, verify=False)
                
                if response.status_code == 200:
                    if self.validate_content(response):
                        result.update({
                            'working': True,
                            'final_url': response.url,
                            'protocol': protocol,
                            'status_code': response.status_code
                        })
                        
                        self.analyze_content(response, result)
                        self.consecutive_failures = 0  # Reset on success
                        return result
                        
            except requests.exceptions.ConnectionError as e:
                # This might be a connectivity issue
                self.consecutive_failures += 1
                if self.consecutive_failures >= 3:
                    result['error'] = 'Connection error - possible connectivity issue'
                    result['failed_due_to_connectivity'] = True
                else:
                    result['error'] = f'Connection error: {str(e)}'
                continue
            except Exception as e:
                result['error'] = str(e)
                continue
        
        return result

    def validate_content(self, response):
        """Validate that the response contains meaningful content"""
        try:
            if len(response.content) < 50:
                return False
            
            content_type = response.headers.get('content-type', '').lower()
            if not any(ct in content_type for ct in ['text/html', 'text/plain']):
                return False
            
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            
            # Check for minimal content
            cleaned_text = ' '.join(text.split())
            if len(cleaned_text) < 100:
                return False
                
            # Check for launching soon type messages
            launching_phrases = [
                'launching soon', 'coming soon', 'under construction',
                'be right back', 'website will be available'
            ]
            if any(phrase in text.lower() for phrase in launching_phrases):
                return False
            
            critical_errors = [
                'page not found', '404 not found', '403 forbidden', 
                '500 internal server error', 'bad gateway'
            ]
            
            text_lower = text.lower()
            if any(error in text_lower for error in critical_errors):
                return False
            
            words = text.split()
            return len(words) >= 5
            
        except Exception:
            return False

    def analyze_content(self, response, result):
        """Analyze webpage content - FIXED VERSION with new detections"""
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                result['title'] = title_tag.get_text().strip()
            
            # Extract description
            desc_tag = soup.find('meta', attrs={'name': 'description'})
            if desc_tag:
                result['description'] = desc_tag.get('content', '').strip()
            
            page_text = soup.get_text()
            
            # NEW: Detect hacked websites
            hacked_detection = self.detect_hacked_website(soup, page_text, response)
            result['is_hacked'] = hacked_detection['is_hacked']
            result['hacked_indicators'] = hacked_detection['indicators']
            result['hacked_confidence'] = hacked_detection['confidence']
            
            # NEW: Detect language
            language_detection = self.detect_website_language(soup, page_text)
            result['primary_language'] = language_detection['primary_language']
            result['is_non_english'] = language_detection['is_non_english']
            result['language_confidence'] = language_detection['confidence']
            
            # If website is hacked, mark it appropriately
            if result['is_hacked']:
                result['is_parked'] = False  # Not parked, but hacked
                result['is_business'] = False  # Don't treat hacked sites as businesses
                result['error'] = 'Website appears to be hacked'
                return  # Don't continue analysis on hacked sites
            
            # Check if parked
            result['is_parked'] = self.is_parked_domain(page_text.lower(), result.get('title', ''))
            
            # Always run classification for working websites
            if not result['is_parked']:
                result['is_business'] = self.is_business_website(soup, page_text.lower())
            else:
                result['is_business'] = False
            
            # Run industry and size classification for all working sites
            try:
                industry_result = self.classify_industry(page_text.lower(), result.get('title', ''), result.get('description', ''))
                result['industry_type'] = industry_result.get('industry', '')
                result['industry_confidence'] = industry_result.get('confidence', 0)
            except Exception as e:
                logger.error(f"Error in industry classification for {result['domain']}: {e}")
                result['industry_type'] = ''
                result['industry_confidence'] = 0
            
            # Only classify company size for business websites
            if result['is_business']:
                try:
                    size_result = self.classify_company_size(soup, page_text.lower(), result.get('title', ''), result.get('description', ''))
                    result['company_size'] = size_result.get('size', '')
                    result['size_confidence'] = size_result.get('confidence', 0)
                    result['size_details'] = size_result.get('details', {})
                except Exception as e:
                    logger.error(f"Error in company size classification for {result['domain']}: {e}")
                    result['company_size'] = ''
                    result['size_confidence'] = 0
                    result['size_details'] = {}
                
                # Extract business info for business websites
                try:
                    result['business_info'] = self.extract_business_info(soup)
                except Exception as e:
                    logger.error(f"Error extracting business info for {result['domain']}: {e}")
                    result['business_info'] = {}
            else:
                result['company_size'] = ''
                result['size_confidence'] = 0
                result['size_details'] = {}
                result['business_info'] = {}
            
            # For vacation rental industry, use enhanced classification
            if result['industry_type'] == 'vacation_rental':
                try:
                    vr_classification = self.enhanced_classify_vacation_rental_business(
                        soup, page_text.lower(), result.get('title', ''), 
                        result.get('description', ''), result.get('final_url', ''),
                        result.get('business_info', {})
                    )
                    
                    # Update result with enhanced classification
                    result['vr_business_model'] = vr_classification['business_model']
                    result['is_target_customer'] = vr_classification['is_target_customer']
                    result['vr_priority'] = vr_classification['priority']
                    result['vr_target_score'] = vr_classification['target_score']
                    result['vr_target_factors'] = vr_classification['target_factors']
                    result['vr_model_confidence'] = vr_classification['model_confidence']
                    result['vr_property_count'] = vr_classification['property_count']
                    result['vr_property_count_confidence'] = vr_classification['property_count_confidence']
                    result['vr_decision_maker_accessible'] = vr_classification['decision_maker_accessible']
                    result['vr_decision_maker_score'] = vr_classification['decision_maker_score']
                    result['vr_needs_website_upgrade'] = vr_classification['needs_website_upgrade']
                    result['vr_upgrade_indicators'] = vr_classification['upgrade_indicators']
                    result['vr_property_type'] = vr_classification['property_type']
                    result['vr_geographic_scope'] = vr_classification['geographic_scope']
                    result['vr_exclusion_reason'] = vr_classification['exclusion_reason']
                    
                except Exception as e:
                    logger.error(f"Error in enhanced VR classification for {result['domain']}: {e}")
                    # Set default values
                    result['vr_business_model'] = ''
                    result['vr_exclusion_reason'] = ''
                    result['is_target_customer'] = ''
                    result['vr_model_confidence'] = 0
                    result['vr_priority'] = ''
                    result['vr_target_score'] = 0
                    result['vr_target_factors'] = []
                    result['vr_property_count'] = ''
                    result['vr_property_count_confidence'] = 0
                    result['vr_decision_maker_accessible'] = ''
                    result['vr_decision_maker_score'] = 0
                    result['vr_needs_website_upgrade'] = False
                    result['vr_upgrade_indicators'] = []
                    result['vr_property_type'] = ''
                    result['vr_geographic_scope'] = ''
            else:
                # For non-vacation rental industries, set default values
                result['vr_business_model'] = ''
                result['vr_exclusion_reason'] = ''
                result['is_target_customer'] = ''
                result['vr_model_confidence'] = 0
                result['vr_priority'] = ''
                result['vr_target_score'] = 0
                result['vr_target_factors'] = []
                result['vr_property_count'] = ''
                result['vr_property_count_confidence'] = 0
                result['vr_decision_maker_accessible'] = ''
                result['vr_decision_maker_score'] = 0
                result['vr_needs_website_upgrade'] = False
                result['vr_upgrade_indicators'] = []
                result['vr_property_type'] = ''
                result['vr_geographic_scope'] = ''
            
        except Exception as e:
            logger.error(f"Error analyzing content: {e}")

    # Also update the is_parked_domain function to be more content-aware
    def is_parked_domain(self, page_text, title):
        """Check if domain is parked - with vacation rental awareness"""
        text_to_check = (page_text + ' ' + title.lower()).lower()
        
        # First, check if it's actually a vacation rental site with minimal content
        vr_keywords = ['vacation rental', 'holiday home', 'beach house', 'cabin', 
                       'cottage', 'villa', 'property rental', 'book now', 'check availability']
        
        vr_score = sum(1 for keyword in vr_keywords if keyword in text_to_check)
        
        # If it has vacation rental keywords, be more lenient
        if vr_score >= 2:
            # Only mark as parked if VERY clear indicators
            strong_parked_indicators = [
                'domain for sale', 'buy this domain', 'this domain is for sale',
                'domain parking', 'hugedomains', 'godaddy auction'
            ]
            return any(indicator in text_to_check for indicator in strong_parked_indicators)
        
        # Otherwise, use normal parked detection
        word_count = len(page_text.split())
        if word_count < 100:
            minimal_patterns = [
                'domain', 'sale', 'buy', 'purchase', 'available', 'premium',
                'coming soon', 'under construction', 'placeholder'
            ]
            pattern_count = sum(1 for pattern in minimal_patterns if pattern in text_to_check)
            if pattern_count >= 3:
                return True
        
        # Check original indicators
        for indicator in self.parked_indicators:
            if indicator in text_to_check:
                return True
        
        return False

    def crawl_additional_pages(self, base_url, soup):
        """Crawl key pages to gather more content"""
        try:
            from urllib.parse import urljoin, urlparse
            
            important_pages = []
            
            # Find links to important pages
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                text = link.get_text().lower()
                
                # Look for important page indicators
                if any(keyword in text or keyword in href for keyword in [
                    'properties', 'rentals', 'rates', 'availability', 
                    'amenities', 'gallery', 'photos', 'about', 'contact',
                    'our home', 'our property', 'the house', 'the property'
                ]):
                    full_url = urljoin(base_url, link.get('href'))
                    # Only crawl same domain
                    if urlparse(full_url).netloc == urlparse(base_url).netloc:
                        if full_url not in important_pages and full_url != base_url:
                            important_pages.append(full_url)
            
            # Crawl up to 5 additional pages
            additional_content = []
            for url in important_pages[:5]:
                try:
                    response = self.session.get(url, timeout=5, verify=False)
                    if response.status_code == 200:
                        page_soup = BeautifulSoup(response.content, 'html.parser')
                        page_text = page_soup.get_text()
                        # Only add if it has substantial content
                        if len(page_text.split()) > 100:
                            additional_content.append(page_text)
                            logger.debug(f"Crawled additional page: {url}")
                except Exception as e:
                    logger.debug(f"Failed to crawl {url}: {e}")
                    continue
            
            return ' '.join(additional_content)
            
        except Exception as e:
            logger.error(f"Error in crawl_additional_pages: {e}")
            return ''

        # 2. ADD HACKED WEBSITE DETECTION
    def detect_hacked_website(self, soup, page_text, response):
        """Detect if website has been hacked"""
        try:
            hacked_score = 0
            hacked_indicators = []
            
            # Common hacking indicators
            hacking_keywords = [
                # Pharmacy spam
                'viagra', 'cialis', 'levitra', 'pharmacy', 'prescription drugs',
                'buy pills online', 'cheap meds', 'online pharmacy', 'medications',
                
                # Casino/gambling spam
                'online casino', 'poker online', 'slots', 'gambling', 'bet online',
                'play casino', 'win money', 'jackpot', 'betting site',
                
                # Porn/adult content (when unexpected)
                'xxx', 'porn', 'adult content', 'sex', 'nude', 'escorts',
                
                # Loan/financial scams
                'payday loan', 'quick loan', 'fast cash', 'instant approval',
                'bad credit loan', 'no credit check', 'guaranteed approval',
                
                # Crypto scams
                'bitcoin mining', 'crypto investment', 'blockchain profit',
                'cryptocurrency trading', 'btc doubler', 'ethereum giveaway',
                
                # SEO spam
                'seo services', 'backlinks for sale', 'link building',
                'google ranking', 'first page guaranteed',
                
                # Other common spam
                'replica watches', 'fake documents', 'essay writing service',
                'weight loss pills', 'work from home', 'make money online'
            ]
            
            # Check for suspicious redirects
            if response and response.history:
                if len(response.history) > 2:  # Multiple redirects
                    hacked_score += 20
                    hacked_indicators.append("Multiple suspicious redirects")
                
                # Check if redirected to completely different domain
                original_domain = urlparse(response.history[0].url).netloc if response.history else ''
                final_domain = urlparse(response.url).netloc
                if original_domain and final_domain and original_domain != final_domain:
                    if not any(brand in final_domain for brand in ['google', 'facebook', 'microsoft']):
                        hacked_score += 30
                        hacked_indicators.append(f"Redirected to different domain: {final_domain}")
            
            # Check page content for spam keywords
            text_lower = page_text.lower()
            spam_found = []
            for keyword in hacking_keywords:
                if keyword in text_lower:
                    # Check if it's contextually appropriate (e.g., pharmacy site mentioning medications)
                    if 'pharmacy' not in result.get('industry_type', '').lower():
                        spam_found.append(keyword)
                        hacked_score += 10
            
            if spam_found:
                hacked_indicators.append(f"Spam keywords found: {', '.join(spam_found[:5])}")
            
            # Check for hidden/invisible content
            hidden_divs = soup.find_all(['div', 'span'], style=re.compile(r'display:\s*none|visibility:\s*hidden'))
            if len(hidden_divs) > 5:
                hacked_score += 15
                hacked_indicators.append(f"Multiple hidden elements ({len(hidden_divs)})")
            
            # Check for suspicious scripts
            scripts = soup.find_all('script')
            suspicious_scripts = 0
            for script in scripts:
                script_text = script.string or ''
                if any(term in script_text.lower() for term in ['eval(', 'base64', 'fromcharcode', 'unescape']):
                    suspicious_scripts += 1
            
            if suspicious_scripts > 2:
                hacked_score += 20
                hacked_indicators.append(f"Suspicious scripts detected ({suspicious_scripts})")
            
            # Check for iframe injections
            iframes = soup.find_all('iframe')
            suspicious_iframes = []
            for iframe in iframes:
                src = iframe.get('src', '')
                # Check for suspicious iframe sources
                if src and not any(safe in src for safe in ['youtube', 'vimeo', 'google', 'facebook']):
                    if iframe.get('style') and ('display:none' in iframe.get('style') or 'visibility:hidden' in iframe.get('style')):
                        suspicious_iframes.append(src)
            
            if suspicious_iframes:
                hacked_score += 25
                hacked_indicators.append(f"Hidden iframes detected: {len(suspicious_iframes)}")
            
            # Check for out-of-place content
            title = result.get('title', '').lower()
            if title and page_text:
                # If title suggests one thing but content is completely different
                if ('vacation rental' in title and any(spam in text_lower for spam in ['viagra', 'casino', 'porn'])):
                    hacked_score += 30
                    hacked_indicators.append("Content doesn't match title/industry")
            
            # Determine if likely hacked
            is_hacked = hacked_score >= 40
            confidence = min(95, hacked_score)
            
            return {
                'is_hacked': is_hacked,
                'hacked_score': hacked_score,
                'confidence': confidence,
                'indicators': hacked_indicators
            }
            
        except Exception as e:
            logger.error(f"Error detecting hacked website: {e}")
            return {
                'is_hacked': False,
                'hacked_score': 0,
                'confidence': 0,
                'indicators': []
            }

    # 3. ADD LANGUAGE DETECTION
    def detect_website_language(self, soup, page_text):
        """Detect the primary language of the website"""
        try:
            languages_detected = []
            confidence = 0
            
            # 1. Check HTML lang attribute
            html_tag = soup.find('html')
            if html_tag and html_tag.get('lang'):
                lang_code = html_tag.get('lang')[:2].lower()  # Get first 2 chars (e.g., 'en' from 'en-US')
                languages_detected.append(('html_lang', lang_code))
                confidence += 30
            
            # 2. Check meta language tags
            meta_langs = soup.find_all('meta', attrs={'http-equiv': re.compile('content-language', re.I)})
            for meta in meta_langs:
                content = meta.get('content', '').lower()[:2]
                if content:
                    languages_detected.append(('meta_lang', content))
                    confidence += 20
            
            # 3. Simple language detection based on common words
            language_patterns = {
                'english': {
                    'words': ['the', 'and', 'for', 'with', 'this', 'that', 'from', 'have', 'will', 'about'],
                    'chars': set('abcdefghijklmnopqrstuvwxyz')
                },
                'spanish': {
                    'words': ['para', 'con', 'por', 'que', 'una', 'los', 'las', 'este', 'esta', 'más'],
                    'chars': set('ñáéíóúü')
                },
                'french': {
                    'words': ['pour', 'avec', 'dans', 'sur', 'les', 'des', 'une', 'est', 'être', 'plus'],
                    'chars': set('àâçèéêëîïôùûüÿœæ')
                },
                'german': {
                    'words': ['der', 'die', 'das', 'und', 'für', 'mit', 'ist', 'auf', 'ein', 'eine'],
                    'chars': set('äöüß')
                },
                'italian': {
                    'words': ['per', 'con', 'del', 'della', 'che', 'una', 'sono', 'più', 'anche', 'come'],
                    'chars': set('àèéìòù')
                },
                'portuguese': {
                    'words': ['para', 'com', 'por', 'que', 'uma', 'são', 'não', 'mais', 'foi', 'está'],
                    'chars': set('ãõçáéíóúâêô')
                },
                'dutch': {
                    'words': ['van', 'het', 'een', 'voor', 'met', 'aan', 'bij', 'ook', 'maar', 'deze'],
                    'chars': set('ëï')
                },
                'chinese': {
                    'words': [],
                    'chars': None,  # Check Unicode range instead
                    'unicode_range': (0x4E00, 0x9FFF)  # CJK Unified Ideographs
                },
                'japanese': {
                    'words': [],
                    'chars': None,
                    'unicode_range': [(0x3040, 0x309F), (0x30A0, 0x30FF)]  # Hiragana and Katakana
                },
                'arabic': {
                    'words': [],
                    'chars': None,
                    'unicode_range': (0x0600, 0x06FF)  # Arabic
                },
                'russian': {
                    'words': ['и', 'в', 'на', 'с', 'по', 'для', 'не', 'что', 'это', 'как'],
                    'chars': None,
                    'unicode_range': (0x0400, 0x04FF)  # Cyrillic
                }
            }
            
            # Count occurrences of language-specific patterns
            text_lower = page_text.lower()
            words = text_lower.split()
            language_scores = {}
            
            for lang, patterns in language_patterns.items():
                score = 0
                
                # Check common words
                if patterns['words']:
                    for word in patterns['words']:
                        score += words.count(word)
                
                # Check special characters
                if patterns.get('chars'):
                    for char in text_lower:
                        if char in patterns['chars']:
                            score += 0.5
                
                # Check Unicode ranges
                if patterns.get('unicode_range'):
                    for char in page_text:
                        if isinstance(patterns['unicode_range'], list):
                            # Multiple ranges (e.g., Japanese)
                            for range_tuple in patterns['unicode_range']:
                                if range_tuple[0] <= ord(char) <= range_tuple[1]:
                                    score += 1
                        else:
                            # Single range
                            if patterns['unicode_range'][0] <= ord(char) <= patterns['unicode_range'][1]:
                                score += 1
                
                if score > 0:
                    language_scores[lang] = score
            
            # Determine primary language
            if language_scores:
                primary_language = max(language_scores, key=language_scores.get)
                total_score = sum(language_scores.values())
                if total_score > 0:
                    confidence += (language_scores[primary_language] / total_score) * 50
            else:
                primary_language = 'unknown'
            
            # Combine detection methods
            if languages_detected:
                # Prefer HTML lang attribute if available
                for method, lang in languages_detected:
                    if method == 'html_lang':
                        lang_map = {
                            'en': 'english', 'es': 'spanish', 'fr': 'french',
                            'de': 'german', 'it': 'italian', 'pt': 'portuguese',
                            'nl': 'dutch', 'zh': 'chinese', 'ja': 'japanese',
                            'ar': 'arabic', 'ru': 'russian'
                        }
                        if lang in lang_map:
                            primary_language = lang_map[lang]
                            confidence = min(95, confidence + 20)
            
            # Check if it's a non-English site
            is_non_english = primary_language != 'english' and primary_language != 'unknown'
            
            return {
                'primary_language': primary_language,
                'is_non_english': is_non_english,
                'confidence': min(95, confidence),
                'language_scores': language_scores,
                'detected_methods': languages_detected
            }
            
        except Exception as e:
            logger.error(f"Error detecting language: {e}")
            return {
                'primary_language': 'unknown',
                'is_non_english': False,
                'confidence': 0,
                'language_scores': {},
                'detected_methods': []
            }    

    def detect_third_party_listing(self, soup, page_text, title, description, final_url):
        """Detect if this is a third-party listing page rather than a direct property owner website"""
        try:
            all_text = (page_text + ' ' + (title or '') + ' ' + (description or '') + ' ' + (final_url or '')).lower()
            
            detection_scores = {
                'url_patterns': 0,
                'content_indicators': 0,
                'template_indicators': 0,
                'navigation_indicators': 0,
                'generic_contact': 0,
                'marketplace_features': 0
            }
            
            listing_data = self.vacation_rental_business_models['third_party_listings']
            
            # 1. URL Pattern Analysis (High Weight)
            url_to_check = final_url or ''
            for pattern in listing_data['url_patterns']:
                if re.search(pattern, url_to_check, re.IGNORECASE):
                    detection_scores['url_patterns'] += 15  # High score for URL patterns
            
            # 2. Content Indicators Analysis
            for indicator in listing_data['content_indicators']:
                count = all_text.count(indicator)
                if count > 0:
                    detection_scores['content_indicators'] += count * 3
            
            # 3. Template Structure Indicators
            for indicator in listing_data['template_indicators']:
                if indicator in all_text:
                    detection_scores['template_indicators'] += 4
            
            # 4. Navigation/Browse Features
            for indicator in listing_data['navigation_indicators']:
                if indicator in all_text:
                    detection_scores['navigation_indicators'] += 5
            
            # 5. Generic Contact Information
            for indicator in listing_data['generic_contact_indicators']:
                if indicator in all_text:
                    detection_scores['generic_contact'] += 6
            
            # 6. Advanced Detection: Page Structure Analysis
            marketplace_structure_score = self.analyze_marketplace_structure(soup, all_text)
            detection_scores['marketplace_features'] = marketplace_structure_score
            
            # 7. Cross-reference with known platforms
            platform_score = self.check_known_listing_platforms(final_url, all_text)
            detection_scores['marketplace_features'] += platform_score
            
            # Calculate total score and confidence
            total_score = sum(detection_scores.values())
            
            # Weighted scoring - URL patterns are most important
            weighted_score = (
                detection_scores['url_patterns'] * 2.0 +  # URL patterns are strongest indicator
                detection_scores['content_indicators'] * 1.5 +
                detection_scores['template_indicators'] * 1.2 +
                detection_scores['navigation_indicators'] * 1.3 +
                detection_scores['generic_contact'] * 1.4 +
                detection_scores['marketplace_features'] * 1.6
            )
            
            # Determine if it's a third-party listing
            is_listing = False
            confidence = 0
            
            if weighted_score >= 25:  # High threshold for confidence
                is_listing = True
                confidence = min(95, weighted_score * 2)
            elif weighted_score >= 15:  # Medium threshold
                is_listing = True
                confidence = min(85, weighted_score * 2.5)
            elif detection_scores['url_patterns'] >= 15:  # URL pattern alone is strong indicator
                is_listing = True
                confidence = 90
            
            return {
                'is_third_party_listing': is_listing,
                'confidence': round(confidence),
                'detection_scores': detection_scores,
                'total_score': total_score,
                'weighted_score': round(weighted_score, 1),
                'evidence_found': [k for k, v in detection_scores.items() if v > 0]
            }
            
        except Exception as e:
            logger.error(f"Error detecting third-party listing: {e}")
            return {
                'is_third_party_listing': False,
                'confidence': 0,
                'detection_scores': {},
                'total_score': 0,
                'weighted_score': 0,
                'evidence_found': []
            }

    def analyze_marketplace_structure(self, soup, page_text):
        """Analyze page structure for marketplace indicators"""
        score = 0
        
        try:
            # Check for booking widgets/forms
            booking_forms = soup.find_all(['form'], class_=re.compile(r'book|reserv|avail', re.I))
            if len(booking_forms) > 0:
                score += 8
            
            # Look for calendar widgets
            calendar_elements = soup.find_all(attrs={'class': re.compile(r'calendar|datepicker|availability', re.I)})
            if len(calendar_elements) > 0:
                score += 6
            
            # Check for review sections
            review_elements = soup.find_all(attrs={'class': re.compile(r'review|rating|feedback', re.I)})
            if len(review_elements) > 2:
                score += 5
            
            # Look for property gallery/slideshow
            gallery_elements = soup.find_all(attrs={'class': re.compile(r'gallery|slideshow|carousel|photo', re.I)})
            if len(gallery_elements) > 0:
                score += 4
            
            # Check for amenities lists
            amenity_elements = soup.find_all(attrs={'class': re.compile(r'amenity|amenities|feature', re.I)})
            if len(amenity_elements) > 3:
                score += 3
            
            # Look for property specifications (beds, baths, etc.)
            spec_patterns = ['bed', 'bath', 'sleep', 'guest', 'sqft', 'sq ft']
            spec_count = sum(1 for pattern in spec_patterns if pattern in page_text)
            if spec_count >= 4:
                score += 5
            
            # Check for pricing display
            price_elements = soup.find_all(attrs={'class': re.compile(r'price|rate|cost|fee', re.I)})
            if len(price_elements) > 2:
                score += 4
            
            # Look for host/owner profile sections
            host_elements = soup.find_all(attrs={'class': re.compile(r'host|owner|manager', re.I)})
            if len(host_elements) > 1:
                score += 6
            
            # Check for similar properties section
            similar_elements = soup.find_all(attrs={'class': re.compile(r'similar|related|recommend', re.I)})
            if len(similar_elements) > 0:
                score += 7
            
            # Look for map integration
            map_elements = soup.find_all(['iframe', 'div'], attrs={'class': re.compile(r'map|location', re.I)})
            if len(map_elements) > 0:
                score += 3
            
        except Exception as e:
            logger.debug(f"Error analyzing marketplace structure: {e}")
        
        return score

    def check_known_listing_platforms(self, url, page_text):
        """Check against known listing platforms and patterns"""
        score = 0
        
        # Known listing platform domains
        listing_platforms = [
            'airbnb', 'vrbo', 'booking', 'expedia', 'tripadvisor', 'homeaway',
            'vacasa', 'turnkey', 'awaze', 'novasol', 'rentals.com', 'flipkey',
            'redawning', 'vacationrenter', 'hometogo', 'rentbyowner',
            'holidaylettings', 'homelidays', 'wimdu', 'citybase', 'uktvillas',
            'villasofthepworld', 'luxuryretreats', 'onefinestay', 'sonder',
            'vacationhomerentals', 'beachhouse', 'mountaincabingetaway'
        ]
        
        url_lower = (url or '').lower()
        for platform in listing_platforms:
            if platform in url_lower:
                score += 20  # High score for known platforms
                break
        
        # Check for listing platform branding in content
        platform_indicators = [
            'powered by airbnb', 'vrbo listing', 'booking.com property',
            'tripadvisor rental', 'homeaway property', 'vacasa managed',
            'listed on', 'featured on', 'available on', 'book through',
            'reserve on', 'property management by', 'managed by'
        ]
        
        for indicator in platform_indicators:
            if indicator in page_text:
                score += 8
        
        return score

    def classify_vacation_rental_business_model(self, soup, page_text, title, description, final_url):
        """Classify vacation rental business model to identify actual rental operators vs service providers vs listings"""
        try:
            all_text = (page_text + ' ' + (title or '') + ' ' + (description or '') + ' ' + (final_url or '')).lower()
            
            # FIRST: Check if this is a third-party listing (HIGHEST PRIORITY)
            listing_detection = self.detect_third_party_listing(soup, page_text, title, description, final_url)
            
            if listing_detection['is_third_party_listing'] and listing_detection['confidence'] > 70:
                return {
                    'business_model': 'third_party_listing',
                    'exclusion_reason': 'third_party_listing',
                    'confidence': listing_detection['confidence'],
                    'is_target_customer': False,
                    'listing_detection': listing_detection,
                    'evidence': listing_detection['evidence_found']
                }
            
            # Check URL for major platforms first
            for platform_url in self.vacation_rental_business_models['marketplace_platforms']['url_indicators']:
                if platform_url in (final_url or '').lower():
                    return {
                        'business_model': 'marketplace_platform',
                        'exclusion_reason': 'marketplace_platform',
                        'confidence': 95,
                        'is_target_customer': False,
                        'platform_detected': platform_url
                    }
            
            # Score different business models
            scores = {
                'marketplace_platform': 0,
                'b2b_service_provider': 0,
                'marketing_service': 0,
                'aggregator_site': 0,
                'direct_rental_operator': 0,
                'third_party_listing': listing_detection['weighted_score'] if listing_detection['is_third_party_listing'] else 0
            }
            
            # Check marketplace platform indicators
            for keyword in self.vacation_rental_business_models['marketplace_platforms']['keywords']:
                count = all_text.count(keyword)
                if count > 0:
                    if keyword in ['airbnb', 'vrbo', 'booking.com', 'expedia']:
                        scores['marketplace_platform'] += count * 10  # Strong indicators
                    else:
                        scores['marketplace_platform'] += count * 3
            
            # Check B2B service provider indicators
            for keyword in self.vacation_rental_business_models['b2b_service_providers']['keywords']:
                count = all_text.count(keyword)
                if count > 0:
                    if keyword in ['property management software', 'vacation rental software', 'pms']:
                        scores['b2b_service_provider'] += count * 8
                    elif keyword in ['tools', 'software', 'platform', 'solution']:
                        scores['b2b_service_provider'] += count * 4
                    else:
                        scores['b2b_service_provider'] += count * 2
            
            # Check marketing/lead gen indicators
            for keyword in self.vacation_rental_business_models['marketing_lead_gen']['keywords']:
                count = all_text.count(keyword)
                if count > 0:
                    scores['marketing_service'] += count * 3
            
            # Check aggregator indicators
            for keyword in self.vacation_rental_business_models['aggregator_listing_sites']['keywords']:
                count = all_text.count(keyword)
                if count > 0:
                    scores['aggregator_site'] += count * 3
            
            # Check for actual rental operator indicators
            for keyword in self.vacation_rental_business_models['actual_rental_operators']['positive_indicators']:
                count = all_text.count(keyword)
                if count > 0:
                    if keyword in ['our properties', 'our rentals', 'our vacation homes', 'direct owner', 'property owner']:
                        scores['direct_rental_operator'] += count * 10  # Strongest indicators
                    elif keyword in ['family owned', 'locally owned', 'personal service', 'no booking fees']:
                        scores['direct_rental_operator'] += count * 8
                    else:
                        scores['direct_rental_operator'] += count * 3
            
            # Additional signals for direct operators
            # Look for specific property types
            property_types = ['beach house', 'mountain cabin', 'lake house', 'ski chalet', 'downtown condo']
            for prop_type in property_types:
                if prop_type in all_text:
                    scores['direct_rental_operator'] += 3
            
            # Look for local area mentions (indicates local business)
            location_phrases = ['located in', 'based in', 'serving', 'minutes from', 'close to', 'near']
            for phrase in location_phrases:
                if phrase in all_text:
                    scores['direct_rental_operator'] += 2
            
            # PENALTY: If listing detection found significant evidence, penalize direct operator score
            if listing_detection['confidence'] > 50:
                scores['direct_rental_operator'] = max(0, scores['direct_rental_operator'] - listing_detection['confidence'] // 10)
            
            # Determine the winner
            if max(scores.values()) == 0:
                return {
                    'business_model': 'unknown',
                    'exclusion_reason': None,
                    'confidence': 0,
                    'is_target_customer': True,  # Default to potential customer if unclear
                    'scores': scores,
                    'listing_detection': listing_detection
                }
            
            top_model = max(scores, key=scores.get)
            top_score = scores[top_model]
            confidence = min(95, (top_score / sum(scores.values()) * 100) if sum(scores.values()) > 0 else 0)
            
            # Special handling for third-party listings
            if top_model == 'third_party_listing' or listing_detection['confidence'] > 60:
                return {
                    'business_model': 'third_party_listing',
                    'exclusion_reason': 'third_party_listing',
                    'confidence': max(confidence, listing_detection['confidence']),
                    'is_target_customer': False,
                    'scores': scores,
                    'listing_detection': listing_detection
                }
            
            # Determine if this is a target customer
            is_target_customer = top_model == 'direct_rental_operator'
            
            # Get exclusion reason for non-targets
            exclusion_reason = None
            if not is_target_customer:
                exclusion_mapping = {
                    'marketplace_platform': 'marketplace_platform',
                    'b2b_service_provider': 'b2b_service_provider', 
                    'marketing_service': 'marketing_service',
                    'aggregator_site': 'aggregator_site',
                    'third_party_listing': 'third_party_listing'
                }
                exclusion_reason = exclusion_mapping.get(top_model)
            
            return {
                'business_model': top_model,
                'exclusion_reason': exclusion_reason,
                'confidence': round(confidence),
                'is_target_customer': is_target_customer,
                'scores': scores,
                'listing_detection': listing_detection
            }
            
        except Exception as e:
            logger.error(f"Error classifying vacation rental business model: {e}")
            return {
                'business_model': 'unknown',
                'exclusion_reason': None,
                'confidence': 0,
                'is_target_customer': True,
                'scores': {},
                'listing_detection': {}
            }

    def classify_company_size(self, soup, page_text, title, description):
        """Enhanced company size classification with detailed analysis (prioritizing small businesses)"""
        try:
            all_text = (page_text + ' ' + (title or '') + ' ' + (description or '')).lower()
            
            # Initialize scores - SMALL BUSINESS GETS HIGHER MULTIPLIERS
            large_score = 0
            medium_score = 0
            small_score = 0
            
            # Check for LARGE company indicators (these are red flags for vacation rental operators)
            for category, keywords in self.large_company_indicators.items():
                for keyword in keywords:
                    count = all_text.count(keyword)
                    if count > 0:
                        if category == 'listing_platform_keywords':
                            large_score += count * 15  # Strong indicator of listing platform
                        elif category == 'headquarters_indicators':
                            large_score += count * 12  # Strong corporate indicator
                        elif category == 'fortune_keywords':
                            large_score += count * 10  # Public company indicator
                        elif category == 'big_business_indicators':
                            large_score += count * 8   # Corporate communications
                        elif category == 'scale_indicators':
                            large_score += count * 6
                        elif category == 'corporate_structure':
                            large_score += count * 4
                        else:
                            large_score += count * 3
            
            # Check for MEDIUM company indicators
            for category, keywords in self.medium_company_indicators.items():
                for keyword in keywords:
                    count = all_text.count(keyword)
                    if count > 0:
                        if category == 'medium_scale_indicators':
                            medium_score += count * 4
                        else:
                            medium_score += count * 3
            
            # Check for SMALL company indicators (HIGHER SCORES - these are preferred!)
            for category, keywords in self.small_company_indicators.items():
                for keyword in keywords:
                    count = all_text.count(keyword)
                    if count > 0:
                        if category == 'authentic_small_business':
                            small_score += count * 8  # Highest score for authentic small business
                        elif category == 'local_business':
                            small_score += count * 6  # High score for local business
                        elif category == 'personal_touch':
                            small_score += count * 6  # High score for personal service
                        elif category == 'single_location_indicators':
                            small_score += count * 5  # Good score for single location
                        else:
                            small_score += count * 4
            
            # Technology stack analysis
            for keyword in self.tech_indicators['enterprise_tech']:
                if keyword in all_text:
                    large_score += 8  # Enterprise tech = big business
            
            for keyword in self.tech_indicators['enterprise_hosting']:
                if keyword in all_text:
                    large_score += 5
            
            for keyword in self.tech_indicators['small_business_tech']:
                if keyword in all_text:
                    small_score += 6  # Small business tech = good sign
            
            # Enhanced website complexity analysis
            website_metrics = self.analyze_detailed_website_metrics(soup)
            complexity_score = website_metrics.get('complexity_score', 0)
            
            # Complexity scoring (simpler sites = smaller businesses = better for vacation rentals)
            if complexity_score > 40:
                large_score += 15  # Very complex = likely big business
            elif complexity_score > 25:
                large_score += 10  # Complex = medium to large business
            elif complexity_score > 15:
                medium_score += 5  # Moderate complexity = medium business
            else:
                small_score += 8   # Simple site = likely small business (GOOD!)
            
            # Analyze specific website metrics for size indicators
            word_count = website_metrics.get('word_count', 0)
            if word_count > 5000:
                large_score += 10  # Very large website = big business
            elif word_count > 2000:
                medium_score += 5  # Large website = medium business
            elif word_count < 500:
                small_score += 5   # Small website = small business (good sign)
            
            # Navigation complexity
            nav_items = website_metrics.get('navigation_items', 0)
            if nav_items > 50:
                large_score += 8   # Complex navigation = big business
            elif nav_items > 20:
                medium_score += 4  # Moderate navigation = medium business
            elif nav_items < 10:
                small_score += 4   # Simple navigation = small business
            
            # Social media presence analysis (enhanced)
            social_score = self.analyze_social_presence(soup, all_text)
            if social_score > 15:
                large_score += 5   # Heavy social presence = bigger business
            elif social_score > 8:
                medium_score += 3  # Moderate social presence = medium business
            elif social_score > 3:
                small_score += 2   # Light social presence = small business
            
            # Employee count indicators
            employee_indicators = self.detect_employee_count(all_text)
            if employee_indicators['size'] == 'large':
                large_score += 20  # Strong indicator of large business
            elif employee_indicators['size'] == 'medium':
                medium_score += 10 # Medium business indicator
            elif employee_indicators['size'] == 'small':
                small_score += 8   # Small team = good sign
            
            # Location analysis (multiple locations = bigger business)
            location_score = self.analyze_locations(all_text)
            if location_score > 10:
                large_score += location_score  # Many locations = big business
            elif location_score > 5:
                medium_score += location_score # Several locations = medium business
            elif location_score == 0:
                small_score += 3  # Single location implied = small business
            
            # Check for specific large business red flags
            red_flags = [
                'api integration', 'enterprise api', 'developer portal', 'white label',
                'franchise opportunities', 'investor relations', 'press releases',
                'corporate partnerships', 'global expansion', 'ipo', 'acquisition'
            ]
            red_flag_count = sum(1 for flag in red_flags if flag in all_text)
            if red_flag_count > 0:
                large_score += red_flag_count * 5
            
            # Determine final classification
            total_score = large_score + medium_score + small_score
            if total_score == 0:
                return {'size': 'unknown', 'confidence': 0, 'details': {}}
            
            # Calculate percentages
            large_pct = (large_score / total_score) * 100
            medium_pct = (medium_score / total_score) * 100
            small_pct = (small_score / total_score) * 100
            
            # Determine classification (BIAS TOWARD SMALL BUSINESS)
            if small_score > large_score and small_score > medium_score:
                size = 'small_business'
                confidence = min(95, small_pct + 10)  # Bonus confidence for small business
            elif large_score > medium_score and large_score > small_score:
                size = 'large_enterprise'
                confidence = min(95, large_pct)
            elif medium_score > 0:
                size = 'medium_business'
                confidence = min(90, medium_pct)
            else:
                size = 'unknown'
                confidence = 0
            
            # Boost small business classification with additional rules
            if small_score >= 15 and large_score < 10:
                size = 'small_business'
                confidence = min(95, confidence + 15)
            
            # Strong penalties for obvious large business indicators
            if large_score >= 25:
                size = 'large_enterprise'
                confidence = min(95, confidence + 10)
            
            details = {
                'large_score': large_score,
                'medium_score': medium_score,
                'small_score': small_score,
                'employee_indicators': employee_indicators,
                'website_metrics': website_metrics,
                'social_score': social_score,
                'location_score': location_score,
                'red_flag_count': red_flag_count,
                'scoring_breakdown': {
                    'large_percentage': round(large_pct, 1),
                    'medium_percentage': round(medium_pct, 1),
                    'small_percentage': round(small_pct, 1)
                }
            }
            
            return {
                'size': size,
                'confidence': round(confidence),
                'details': details
            }
            
        except Exception as e:
            logger.error(f"Error classifying company size: {e}")
            return {'size': 'unknown', 'confidence': 0, 'details': {}}
    
    def analyze_website_complexity(self, soup):
        """Analyze website complexity as an indicator of company size"""
        try:
            score = 0
            
            # Navigation complexity
            nav_items = soup.find_all(['nav', 'ul', 'li'])
            if len(nav_items) > 20:
                score += 5
            elif len(nav_items) > 10:
                score += 3
            
            # Page structure
            sections = soup.find_all(['section', 'div', 'article'])
            if len(sections) > 50:
                score += 4
            elif len(sections) > 25:
                score += 2
            
            # Forms and interactive elements
            forms = soup.find_all(['form', 'input', 'select', 'textarea'])
            if len(forms) > 15:
                score += 3
            elif len(forms) > 8:
                score += 2
            
            # External resources (scripts, stylesheets)
            scripts = soup.find_all('script', src=True)
            stylesheets = soup.find_all('link', rel='stylesheet')
            if len(scripts) + len(stylesheets) > 20:
                score += 4
            elif len(scripts) + len(stylesheets) > 10:
                score += 2
            
            # Advanced features
            if soup.find(['video', 'audio', 'canvas', 'svg']):
                score += 2
            
            return score
            
        except Exception:
            return 0
    
    def analyze_social_presence(self, soup, page_text):
        """Analyze social media presence"""
        try:
            score = 0
            social_platforms = [
                'facebook', 'twitter', 'linkedin', 'instagram', 'youtube',
                'tiktok', 'pinterest', 'snapchat', 'telegram', 'whatsapp'
            ]
            
            # Check for social media links
            links = soup.find_all('a', href=True)
            social_links = set()
            
            for link in links:
                href = link.get('href', '').lower()
                for platform in social_platforms:
                    if platform in href:
                        social_links.add(platform)
            
            # Score based on number of platforms
            score += len(social_links) * 2
            
            # Check for social sharing buttons
            social_sharing_indicators = ['share', 'tweet', 'like', 'follow']
            for indicator in social_sharing_indicators:
                if indicator in page_text:
                    score += 1
            
            return score
            
        except Exception:
            return 0
    
    def detect_employee_count(self, text):
        """Detect employee count indicators"""
        try:
            # Look for specific employee count mentions
            employee_patterns = [
                (r'(\d+),?(\d+)?\+?\s*employees', 'count'),
                (r'over\s+(\d+),?(\d+)?\s*employees', 'over'),
                (r'more than\s+(\d+),?(\d+)?\s*employees', 'over'),
                (r'(\d+),?(\d+)?\+?\s*team members', 'count'),
                (r'(\d+),?(\d+)?\+?\s*staff', 'count')
            ]
            
            for pattern, type_indicator in employee_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    for match in matches:
                        try:
                            # Handle tuple from regex groups
                            if isinstance(match, tuple):
                                num_str = ''.join(match).replace(',', '')
                            else:
                                num_str = str(match).replace(',', '')
                            
                            if num_str.isdigit():
                                count = int(num_str)
                                if count >= 1000:
                                    return {'size': 'large', 'count': count, 'type': type_indicator}
                                elif count >= 50:
                                    return {'size': 'medium', 'count': count, 'type': type_indicator}
                                elif count >= 1:
                                    return {'size': 'small', 'count': count, 'type': type_indicator}
                        except (ValueError, TypeError):
                            continue
            
            # Look for general size indicators
            if any(indicator in text for indicator in ['thousands of employees', 'million employees']):
                return {'size': 'large', 'count': None, 'type': 'estimate'}
            elif any(indicator in text for indicator in ['hundreds of employees', 'large team']):
                return {'size': 'medium', 'count': None, 'type': 'estimate'}
            elif any(indicator in text for indicator in ['small team', 'boutique', 'family business']):
                return {'size': 'small', 'count': None, 'type': 'estimate'}
            
            return {'size': None, 'count': None, 'type': None}
            
        except Exception:
            return {'size': None, 'count': None, 'type': None}
    
    def analyze_locations(self, text):
        """Analyze number of locations/offices"""
        try:
            score = 0
            
            # Look for multiple locations
            location_patterns = [
                r'(\d+)\s*offices?',
                r'(\d+)\s*locations?',
                r'(\d+)\s*branches?',
                r'(\d+)\s*stores?',
                r'(\d+)\s*facilities',
                r'(\d+)\s*countries',
                r'(\d+)\s*states'
            ]
            
            for pattern in location_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    try:
                        count = int(match)
                        if count >= 100:
                            score += 8
                        elif count >= 50:
                            score += 6
                        elif count >= 10:
                            score += 4
                        elif count >= 5:
                            score += 2
                        elif count >= 2:
                            score += 1
                    except ValueError:
                        continue
            
            # Look for global presence indicators
            global_indicators = [
                'worldwide', 'global presence', 'international offices',
                'offices around the world', 'global locations'
            ]
            
            for indicator in global_indicators:
                if indicator in text:
                    score += 3
            
            return score
            
        except Exception:
            return 0

    def classify_industry(self, page_text, title, description):
        """Classify industry type"""
        try:
            all_text = (page_text + ' ' + (title or '') + ' ' + (description or '')).lower()
            industry_scores = {}
            
            for industry, keywords in self.industry_keywords.items():
                score = 0
                for keyword in keywords:
                    if keyword in all_text:
                        frequency = all_text.count(keyword)
                        if title and keyword in title.lower():
                            score += frequency * 3
                        elif description and keyword in description.lower():
                            score += frequency * 2
                        else:
                            score += frequency
                
                if score > 0:
                    industry_scores[industry] = score
            
            if not industry_scores:
                return {'industry': 'general_business', 'confidence': 0}
            
            top_industry = max(industry_scores, key=industry_scores.get)
            top_score = industry_scores[top_industry]
            confidence = min(95, top_score * 10)
            
            return {'industry': top_industry, 'confidence': round(confidence)}
            
        except Exception as e:
            logger.error(f"Error classifying industry: {e}")
            return {'industry': 'unknown', 'confidence': 0}

    def is_business_website(self, soup, page_text):
        """Check if it's a business website - FIXED VERSION"""
        # Special handling for vacation rental sites
        if any(vr_term in page_text.lower() for vr_term in [
            'vacation rental', 'holiday rental', 'property rental',
            'beach house', 'cabin rental', 'vacation home'
        ]):
            # Lower threshold for VR sites
            vr_business_indicators = [
                'contact', 'email', 'phone', 'book', 'availability',
                'property', 'rental', 'rate', 'price', 'location'
            ]
            vr_content_count = sum(1 for indicator in vr_business_indicators if indicator in page_text.lower())
            
            # If it has VR terms and some business indicators, it's a business
            if vr_content_count >= 2:
                return True
        
        # Original business detection logic
        business_indicators = [
            'about us', 'contact us', 'services', 'products', 'company',
            'business', 'team', 'careers', 'support', 'customer',
            'phone', 'email', 'address', 'location', 'hours'
        ]
        
        nav_elements = soup.find_all(['nav', 'menu'])
        has_navigation = len(nav_elements) > 0
        
        business_content_count = sum(1 for indicator in business_indicators if indicator in page_text.lower())
        
        has_contact = bool(re.search(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', page_text) or 
                          re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', page_text))
        
        score = 0
        if has_navigation: score += 2
        if business_content_count >= 3: score += 2
        if has_contact: score += 2
        if len(page_text.split()) > 50: score += 1  # Lowered from 100 to 50
        
        return score >= 3  # Lowered from 4 to 3


    def extract_business_info(self, soup):
        """Extract comprehensive business information including contact details, social media, and location"""
        info = {}
        page_text = soup.get_text()
        
        # Company name - try multiple sources
        company_name = None
        h1_tags = soup.find_all('h1')
        if h1_tags:
            company_name = h1_tags[0].get_text().strip()
        
        # Try title tag if h1 is not descriptive
        title_tag = soup.find('title')
        if title_tag and (not company_name or len(company_name) < 3):
            title_text = title_tag.get_text().strip()
            # Clean up common title suffixes
            for suffix in [' - Home', ' | Home', ' - Welcome', ' | Welcome']:
                if suffix in title_text:
                    title_text = title_text.replace(suffix, '')
            company_name = title_text
        
        info['company_name'] = company_name or ''
        
        # Extract all emails
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', page_text)
        # Filter out common generic emails and keep unique ones
        filtered_emails = []
        generic_prefixes = ['info', 'contact', 'admin', 'support', 'hello', 'mail', 'office']
        for email in set(emails):  # Remove duplicates
            email_lower = email.lower()
            # Skip obviously fake or generic emails
            if not any(skip in email_lower for skip in ['noreply', 'donotreply', 'example', 'test', 'spam']):
                filtered_emails.append(email)
        
        info['emails'] = filtered_emails[:5]  # Limit to 5 emails
        info['primary_email'] = filtered_emails[0] if filtered_emails else ''
        
        # Extract all phone numbers with better patterns
        phone_patterns = [
            r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',  # US format
            r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',      # (123) 456-7890
            r'\b\d{3}\.\d{3}\.\d{4}\b',            # 123.456.7890
            r'\+\d{1,3}[-.\s]?\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}',  # International
        ]
        
        phones = []
        for pattern in phone_patterns:
            phones.extend(re.findall(pattern, page_text))
        
        # Clean and deduplicate phones
        cleaned_phones = []
        for phone in set(phones):
            # Remove common non-phone numbers
            if not any(skip in phone for skip in ['111-111-1111', '123-456-7890', '000-000-0000']):
                cleaned_phones.append(phone.strip())
        
        info['phones'] = cleaned_phones[:3]  # Limit to 3 phones
        info['primary_phone'] = cleaned_phones[0] if cleaned_phones else ''
        
        # Extract physical address
        address = self.extract_address(soup, page_text)
        info['address'] = address
        
        # Extract location and country information
        location_info = self.extract_location_and_country(soup, page_text, address)
        info.update(location_info)
        
        # Extract social media links
        social_media = self.extract_social_media_links(soup)
        info['social_media'] = social_media
        
        # Extract business hours
        hours = self.extract_business_hours(page_text)
        info['business_hours'] = hours
        
        # Check for online booking/reservation systems
        booking_indicators = ['book now', 'reserve now', 'schedule appointment', 'book online', 'make reservation']
        info['has_online_booking'] = any(indicator in page_text.lower() for indicator in booking_indicators)
        
        # Extract website complexity metrics
        info['website_metrics'] = self.analyze_detailed_website_metrics(soup)
        
        return info

    def extract_address(self, soup, page_text):
        """Extract physical address from webpage"""
        address_patterns = [
            # Street address patterns
            r'\d+\s+[A-Za-z0-9\s,.-]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|Court|Ct|Place|Pl)\s*,?\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s+\d{5}',
            r'\d+\s+[A-Za-z0-9\s,.-]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}',
            # PO Box patterns
            r'P\.?O\.?\s+Box\s+\d+,?\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s+\d{5}',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                # Return the first reasonable looking address
                for match in matches:
                    if len(match) > 20:  # Reasonable address length
                        return match.strip()
        
        # Try to find address in structured data or contact sections
        contact_sections = soup.find_all(['div', 'section'], class_=re.compile(r'contact|address|location', re.I))
        for section in contact_sections:
            section_text = section.get_text()
            for pattern in address_patterns:
                matches = re.findall(pattern, section_text, re.IGNORECASE)
                if matches:
                    return matches[0].strip()
        
        return ''

    def extract_location_and_country(self, soup, page_text, address):
        """Extract detailed location and country information"""
        location_info = {
            'country': '',
            'country_confidence': 0,
            'state_province': '',
            'city': '',
            'local_area': '',
            'serves_locations': [],
            'location_indicators': []
        }
        
        try:
            # Country detection patterns
            country_patterns = {
                'United States': {
                    'patterns': [
                        r'\b(?:USA|United States|US|America)\b',
                        r'\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b',  # US ZIP codes
                        r'\b(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s+\d{5}\b',
                    ],
                    'indicators': ['USD', 'dollars', 'ZIP', 'state', 'county'],
                    'states': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
                },
                'Canada': {
                    'patterns': [
                        r'\bCanada\b',
                        r'\b[A-Z]\d[A-Z]\s*\d[A-Z]\d\b',  # Canadian postal codes
                        r'\b(?:AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\b',
                    ],
                    'indicators': ['CAD', 'Canadian', 'province', 'postal code'],
                    'provinces': ['Alberta', 'British Columbia', 'Manitoba', 'New Brunswick', 'Newfoundland and Labrador', 'Northwest Territories', 'Nova Scotia', 'Nunavut', 'Ontario', 'Prince Edward Island', 'Quebec', 'Saskatchewan', 'Yukon']
                },
                'United Kingdom': {
                    'patterns': [
                        r'\b(?:UK|United Kingdom|Britain|England|Scotland|Wales|Northern Ireland)\b',
                        r'\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b',  # UK postcodes
                    ],
                    'indicators': ['£', 'GBP', 'pounds', 'postcode', 'shire', 'county'],
                    'regions': ['England', 'Scotland', 'Wales', 'Northern Ireland']
                },
                'Australia': {
                    'patterns': [
                        r'\bAustralia\b',
                        r'\b\d{4}\s*(?:NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\b',
                    ],
                    'indicators': ['AUD', 'Australian', 'postcode'],
                    'states': ['New South Wales', 'Victoria', 'Queensland', 'Western Australia', 'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory']
                },
                'Germany': {
                    'patterns': [
                        r'\bDeutschland\b|\bGermany\b',
                        r'\b\d{5}\s*(?:Germany|Deutschland)\b',
                    ],
                    'indicators': ['EUR', '€', 'euros', 'German'],
                    'regions': ['Bavaria', 'Baden-Württemberg', 'North Rhine-Westphalia', 'Berlin', 'Hamburg']
                },
                'France': {
                    'patterns': [
                        r'\bFrance\b|\bFrançais\b',
                        r'\b\d{5}\s*France\b',
                    ],
                    'indicators': ['EUR', '€', 'euros', 'French'],
                    'regions': ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice']
                },
                'Spain': {
                    'patterns': [
                        r'\bSpain\b|\bEspaña\b',
                        r'\b\d{5}\s*Spain\b',
                    ],
                    'indicators': ['EUR', '€', 'euros', 'Spanish'],
                    'regions': ['Madrid', 'Barcelona', 'Valencia', 'Seville', 'Bilbao']
                },
                'Italy': {
                    'patterns': [
                        r'\bItaly\b|\bItalia\b',
                        r'\b\d{5}\s*Italy\b',
                    ],
                    'indicators': ['EUR', '€', 'euros', 'Italian'],
                    'regions': ['Rome', 'Milan', 'Naples', 'Turin', 'Florence']
                },
                'Netherlands': {
                    'patterns': [
                        r'\bNetherlands\b|\bHolland\b',
                        r'\b\d{4}\s*[A-Z]{2}\s*Netherlands\b',
                    ],
                    'indicators': ['EUR', '€', 'euros', 'Dutch'],
                    'regions': ['Amsterdam', 'Rotterdam', 'The Hague', 'Utrecht']
                },
                'Mexico': {
                    'patterns': [
                        r'\bMexico\b|\bMéxico\b',
                        r'\b\d{5}\s*Mexico\b',
                    ],
                    'indicators': ['MXN', 'pesos', 'Mexican'],
                    'regions': ['Mexico City', 'Guadalajara', 'Monterrey', 'Cancun', 'Puerto Vallarta']
                }
            }
            
            text_lower = page_text.lower()
            country_scores = {}
            
            # Score countries based on patterns and indicators
            for country, data in country_patterns.items():
                score = 0
                
                # Check patterns
                for pattern in data['patterns']:
                    matches = len(re.findall(pattern, page_text, re.IGNORECASE))
                    if matches > 0:
                        score += matches * 10
                
                # Check indicators
                for indicator in data['indicators']:
                    if indicator.lower() in text_lower:
                        score += 5
                
                # Check states/provinces/regions
                for region in data.get('states', data.get('provinces', data.get('regions', []))):
                    if region.lower() in text_lower:
                        score += 8
                        location_info['state_province'] = region
                
                if score > 0:
                    country_scores[country] = score
            
            # Additional country detection from address
            if address:
                address_lower = address.lower()
                for country, data in country_patterns.items():
                    for pattern in data['patterns']:
                        if re.search(pattern, address, re.IGNORECASE):
                            country_scores[country] = country_scores.get(country, 0) + 15
            
            # Domain-based country detection
            domain_tlds = {
                '.uk': 'United Kingdom',
                '.co.uk': 'United Kingdom',
                '.ca': 'Canada',
                '.au': 'Australia',
                '.com.au': 'Australia',
                '.de': 'Germany',
                '.fr': 'France',
                '.es': 'Spain',
                '.it': 'Italy',
                '.nl': 'Netherlands',
                '.mx': 'Mexico',
                '.com.mx': 'Mexico'
            }
            
            # Check current page URL for TLD
            current_url = soup.find('link', {'rel': 'canonical'})
            if current_url:
                url = current_url.get('href', '')
                for tld, country in domain_tlds.items():
                    if tld in url:
                        country_scores[country] = country_scores.get(country, 0) + 12
            
            # Determine best country match
            if country_scores:
                best_country = max(country_scores, key=country_scores.get)
                best_score = country_scores[best_country]
                total_score = sum(country_scores.values())
                confidence = min(95, (best_score / total_score) * 100) if total_score > 0 else 0
                
                location_info['country'] = best_country
                location_info['country_confidence'] = round(confidence)
            
            # Extract cities and local areas
            location_info['city'] = self.extract_city_names(page_text, location_info['country'])
            location_info['local_area'] = self.extract_local_areas(page_text)
            location_info['serves_locations'] = self.extract_service_areas(page_text)
            
            # Compile location indicators found
            location_indicators = []
            for country, data in country_patterns.items():
                if country in country_scores:
                    for indicator in data['indicators']:
                        if indicator.lower() in text_lower:
                            location_indicators.append(indicator)
            
            location_info['location_indicators'] = list(set(location_indicators))
            
        except Exception as e:
            logger.error(f"Error extracting location information: {e}")
        
        return location_info

    def extract_city_names(self, page_text, country):
        """Extract city names based on country context"""
        cities = []
        
        # Common city patterns by country
        city_databases = {
            'United States': [
                'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
                'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
                'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis',
                'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville',
                'Detroit', 'Oklahoma City', 'Portland', 'Las Vegas', 'Memphis', 'Louisville',
                'Baltimore', 'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno', 'Sacramento',
                'Mesa', 'Kansas City', 'Atlanta', 'Long Beach', 'Colorado Springs', 'Raleigh',
                'Miami', 'Virginia Beach', 'Omaha', 'Oakland', 'Minneapolis', 'Tulsa',
                'Arlington', 'Tampa', 'New Orleans', 'Wichita', 'Cleveland', 'Bakersfield'
            ],
            'Canada': [
                'Toronto', 'Montreal', 'Vancouver', 'Calgary', 'Edmonton', 'Ottawa',
                'Winnipeg', 'Quebec City', 'Hamilton', 'Kitchener', 'London', 'Victoria',
                'Halifax', 'Oshawa', 'Windsor', 'Saskatoon', 'St. Catharines', 'Regina',
                'Kelowna', 'Barrie', 'Sherbrooke', 'Guelph', 'Kanata', 'Abbotsford'
            ],
            'United Kingdom': [
                'London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool', 'Edinburgh',
                'Leeds', 'Sheffield', 'Bristol', 'Cardiff', 'Leicester', 'Belfast',
                'Nottingham', 'Newcastle', 'Brighton', 'Hull', 'Plymouth', 'Stoke',
                'Wolverhampton', 'Derby', 'Swansea', 'Southampton', 'Salford', 'Aberdeen'
            ],
            'Australia': [
                'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Gold Coast',
                'Newcastle', 'Canberra', 'Sunshine Coast', 'Wollongong', 'Geelong',
                'Hobart', 'Townsville', 'Cairns', 'Darwin', 'Toowoomba', 'Ballarat'
            ]
        }
        
        if country in city_databases:
            text_lower = page_text.lower()
            for city in city_databases[country]:
                if city.lower() in text_lower:
                    cities.append(city)
        
        # Generic city pattern detection
        city_patterns = [
            r'\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\blocated\s+in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\bserving\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*[A-Z]{2}\b'
        ]
        
        for pattern in city_patterns:
            matches = re.findall(pattern, page_text)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                if len(match) > 2 and match not in cities:
                    cities.append(match)
        
        return ', '.join(cities[:3])  # Return top 3 cities

    def extract_local_areas(self, page_text):
        """Extract local area indicators"""
        local_patterns = [
            r'\bdowntown\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\bnear\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\bclose\s+to\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\bminutes\s+from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'\bin\s+the\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+area\b',
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+neighborhood\b'
        ]
        
        local_areas = []
        for pattern in local_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                if len(match) > 2 and match not in local_areas:
                    local_areas.append(match)
        
        return ', '.join(local_areas[:3])

    def extract_service_areas(self, page_text):
        """Extract areas served by the business"""
        service_patterns = [
            r'\bserving\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)*)\b',
            r'\bwe\s+serve\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)*)\b',
            r'\bavailable\s+in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)*)\b',
            r'\bdelivering\s+to\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)*)\b'
        ]
        
        service_areas = []
        for pattern in service_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                areas = [area.strip() for area in match.split(',')]
                service_areas.extend(areas)
        
        return list(set(service_areas[:5]))  # Return unique areas, max 5

    def extract_social_media_links(self, soup):
        """Extract social media links from webpage"""
        social_platforms = {
            'facebook': ['facebook.com', 'fb.com'],
            'twitter': ['twitter.com', 'x.com'],
            'instagram': ['instagram.com'],
            'linkedin': ['linkedin.com'],
            'youtube': ['youtube.com', 'youtu.be'],
            'tiktok': ['tiktok.com'],
            'pinterest': ['pinterest.com'],
            'snapchat': ['snapchat.com'],
            'whatsapp': ['whatsapp.com', 'wa.me'],
            'telegram': ['telegram.me', 't.me']
        }
        
        social_links = {}
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link.get('href', '').lower()
            for platform, domains in social_platforms.items():
                if any(domain in href for domain in domains):
                    # Clean up the URL
                    if href.startswith('//'):
                        href = 'https:' + href
                    elif href.startswith('/'):
                        continue  # Skip relative links
                    
                    social_links[platform] = href
                    break
        
        return social_links

    def extract_business_hours(self, page_text):
        """Extract business hours from webpage text"""
        # Common business hours patterns
        hours_patterns = [
            r'(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*[:\s-]*\s*\d{1,2}[:\s]*\d{0,2}\s*(?:am|pm|a\.m\.|p\.m\.)?[\s-]*\d{1,2}[:\s]*\d{0,2}\s*(?:am|pm|a\.m\.|p\.m\.)?',
            r'hours?\s*:?\s*\d{1,2}[:\s]*\d{0,2}\s*(?:am|pm|a\.m\.|p\.m\.)?[\s-]*\d{1,2}[:\s]*\d{0,2}\s*(?:am|pm|a\.m\.|p\.m\.)?',
            r'open\s*:?\s*\d{1,2}[:\s]*\d{0,2}\s*(?:am|pm|a\.m\.|p\.m\.)?[\s-]*\d{1,2}[:\s]*\d{0,2}\s*(?:am|pm|a\.m\.|p\.m\.)?'
        ]
        
        hours_text = []
        text_lower = page_text.lower()
        
        for pattern in hours_patterns:
            matches = re.findall(pattern, text_lower, re.IGNORECASE | re.MULTILINE)
            hours_text.extend(matches)
        
        # Look for "24/7" or "24 hours"
        if '24/7' in text_lower or '24 hours' in text_lower or 'open 24 hours' in text_lower:
            hours_text.append('24/7')
        
        return '; '.join(set(hours_text[:3]))  # Return up to 3 unique hour entries

    def analyze_detailed_website_metrics(self, soup):
        """Analyze detailed website metrics to determine company size"""
        metrics = {}
        
        # Count various elements
        metrics['total_links'] = len(soup.find_all('a'))
        metrics['internal_links'] = len([link for link in soup.find_all('a', href=True) 
                                       if link.get('href', '').startswith('/') or not link.get('href', '').startswith('http')])
        metrics['external_links'] = metrics['total_links'] - metrics['internal_links']
        
        metrics['images'] = len(soup.find_all('img'))
        metrics['forms'] = len(soup.find_all('form'))
        metrics['scripts'] = len(soup.find_all('script'))
        metrics['stylesheets'] = len(soup.find_all('link', rel='stylesheet'))
        
        # Navigation complexity
        nav_elements = soup.find_all(['nav', 'ul', 'ol'])
        metrics['navigation_items'] = sum(len(nav.find_all('li')) for nav in nav_elements)
        
        # Page structure complexity
        metrics['divs'] = len(soup.find_all('div'))
        metrics['sections'] = len(soup.find_all('section'))
        metrics['articles'] = len(soup.find_all('article'))
        
        # Content metrics
        text_content = soup.get_text()
        metrics['word_count'] = len(text_content.split())
        metrics['character_count'] = len(text_content)
        
        # Advanced features
        metrics['videos'] = len(soup.find_all(['video', 'iframe']))
        metrics['interactive_elements'] = len(soup.find_all(['button', 'input', 'select', 'textarea']))
        
        # Calculate complexity score
        complexity_score = 0
        
        # Website size scoring (larger = more complex = bigger business)
        if metrics['word_count'] > 5000:
            complexity_score += 15  # Very large site
        elif metrics['word_count'] > 2000:
            complexity_score += 10  # Large site
        elif metrics['word_count'] > 500:
            complexity_score += 5   # Medium site
        
        if metrics['total_links'] > 100:
            complexity_score += 10
        elif metrics['total_links'] > 50:
            complexity_score += 5
        
        if metrics['navigation_items'] > 50:
            complexity_score += 8
        elif metrics['navigation_items'] > 20:
            complexity_score += 4
        
        if metrics['forms'] > 5:
            complexity_score += 6
        
        if metrics['scripts'] > 20:
            complexity_score += 8
        
        if metrics['images'] > 50:
            complexity_score += 6
        
        metrics['complexity_score'] = complexity_score
        return metrics

    def setup_realtime_csv(self, output_dir):
        """Setup real-time CSV files - ENHANCED VERSION with new fields"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Main results file
        main_file = os.path.join(output_dir, f'realtime_results_{timestamp}.csv')
        self.csv_files['main'] = open(main_file, 'w', newline='', encoding='utf-8')
        
        # ENHANCED: Added new fields for hacked and language detection
        fieldnames = ['domain', 'working', 'final_url', 'protocol', 'status_code', 
                     'title', 'description', 'is_parked', 'is_business', 
                     # NEW FIELDS for hacked and language
                     'is_hacked', 'hacked_indicators', 'hacked_confidence',
                     'primary_language', 'is_non_english', 'language_confidence',
                     # Original fields
                     'industry_type', 'industry_confidence', 'company_size', 'size_confidence', 
                     'vr_business_model', 'vr_exclusion_reason', 'is_target_customer', 'vr_model_confidence',
                     # VR specific fields
                     'vr_priority', 'vr_target_score', 'vr_target_factors', 'vr_property_count',
                     'vr_property_count_confidence', 'vr_decision_maker_accessible', 
                     'vr_decision_maker_score', 'vr_needs_website_upgrade', 'vr_upgrade_indicators',
                     'vr_property_type', 'vr_geographic_scope',
                     # Business info fields
                     'company_name', 'primary_email', 'primary_phone', 'address', 'country',
                     'country_confidence', 'state_province', 'city', 'local_area', 'serves_locations',
                     'social_media_links', 'website_complexity_score', 'word_count', 'total_links', 
                     'has_online_booking', 'error', 'failed_due_to_connectivity', 'processed_at']
        
        self.csv_writers['main'] = csv.DictWriter(self.csv_files['main'], fieldnames=fieldnames)
        self.csv_writers['main'].writeheader()
        self.csv_files['main'].flush()
        
        # High-priority targets file
        high_priority_file = os.path.join(output_dir, f'high_priority_targets_{timestamp}.csv')
        self.csv_files['high_priority'] = open(high_priority_file, 'w', newline='', encoding='utf-8')
        self.csv_writers['high_priority'] = csv.DictWriter(self.csv_files['high_priority'], fieldnames=fieldnames)
        self.csv_writers['high_priority'].writeheader()
        self.csv_files['high_priority'].flush()
        
        print(f"📊 Real-time CSV file created: {main_file}")
        print(f"🎯 High-priority targets file created: {high_priority_file}")

    def write_result_realtime(self, result):
        """Write result to CSV immediately - FIXED INDENTATION"""
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Update stats
            self.stats['total_processed'] += 1
            if result.get('working', False):
                self.stats['working'] += 1
            if result.get('is_business', False):
                self.stats['business'] += 1
            if result.get('is_parked', False):
                self.stats['parked'] += 1
            if result.get('error') and not result.get('failed_due_to_connectivity', False):
                self.stats['failed'] += 1
            
            industry = result.get('industry_type')
            if industry:
                self.stats['industries'][industry] = self.stats['industries'].get(industry, 0) + 1
            
            company_size = result.get('company_size')
            if company_size:
                self.stats['company_sizes'][company_size] = self.stats['company_sizes'].get(company_size, 0) + 1
            
            # Track new VR-specific stats
            if result.get('industry_type') == 'vacation_rental':
                # Track business models
                vr_model = result.get('vr_business_model')
                if vr_model:
                    self.stats['vr_business_models'][vr_model] = self.stats['vr_business_models'].get(vr_model, 0) + 1
                
                # Track priorities
                priority = result.get('vr_priority')
                if priority == 'high':
                    self.stats['high_priority_targets'] += 1
                elif priority == 'medium':
                    self.stats['medium_priority_targets'] += 1
                
                # Track decision maker accessibility
                if result.get('vr_decision_maker_accessible') == 'high':
                    self.stats['decision_maker_accessible'] += 1
                
                # Track website upgrade needs
                if result.get('vr_needs_website_upgrade'):
                    self.stats['website_needs_upgrade'] += 1
                
                # Track property types
                prop_type = result.get('vr_property_type')
                if prop_type:
                    self.stats['vr_property_types'][prop_type] = self.stats['vr_property_types'].get(prop_type, 0) + 1
                
                # Original target customer tracking
                if result.get('is_target_customer') == True:
                    self.stats['target_customers'] += 1
                elif result.get('is_target_customer') == False:
                    exclusion_reason = result.get('vr_exclusion_reason', 'unknown')
                    if exclusion_reason:
                        self.stats['excluded_businesses'][exclusion_reason] = self.stats['excluded_businesses'].get(exclusion_reason, 0) + 1
            
            # Write to CSV - ensure no None values and include new fields
            business_info = result.get('business_info', {})
            social_media = business_info.get('social_media', {})
            website_metrics = business_info.get('website_metrics', {})
            serves_locations = business_info.get('serves_locations', [])
            
            row = {
                'domain': result.get('domain', ''),
                'working': result.get('working', False),
                'final_url': result.get('final_url', '') or '',
                'protocol': result.get('protocol', '') or '',
                'status_code': result.get('status_code', '') or '',
                'title': result.get('title', '') or '',
                'description': result.get('description', '') or '',
                'is_parked': result.get('is_parked', False),
                'is_business': result.get('is_business', False),
                'is_hacked': result.get('is_hacked', False),
                'hacked_indicators': '; '.join(result.get('hacked_indicators', [])),
                'hacked_confidence': result.get('hacked_confidence', 0) or 0,
                'primary_language': result.get('primary_language', '') or '',
                'is_non_english': result.get('is_non_english', False),
                'language_confidence': result.get('language_confidence', 0) or 0,
                'industry_type': result.get('industry_type', '') or '',
                'industry_confidence': result.get('industry_confidence', 0) or 0,
                'company_size': result.get('company_size', '') or '',
                'size_confidence': result.get('size_confidence', 0) or 0,
                'vr_business_model': result.get('vr_business_model', '') or '',
                'vr_exclusion_reason': result.get('vr_exclusion_reason', '') or '',
                'is_target_customer': str(result.get('is_target_customer', '') or ''),
                'vr_model_confidence': result.get('vr_model_confidence', 0) or 0,
                # NEW FIELDS
                'vr_priority': result.get('vr_priority', '') or '',
                'vr_target_score': result.get('vr_target_score', 0) or 0,
                'vr_target_factors': '; '.join(result.get('vr_target_factors', [])),
                'vr_property_count': str(result.get('vr_property_count', '') or ''),
                'vr_property_count_confidence': result.get('vr_property_count_confidence', 0) or 0,
                'vr_decision_maker_accessible': result.get('vr_decision_maker_accessible', '') or '',
                'vr_decision_maker_score': result.get('vr_decision_maker_score', 0) or 0,
                'vr_needs_website_upgrade': str(result.get('vr_needs_website_upgrade', False)),
                'vr_upgrade_indicators': '; '.join(result.get('vr_upgrade_indicators', [])),
                'vr_property_type': result.get('vr_property_type', '') or '',
                'vr_geographic_scope': result.get('vr_geographic_scope', '') or '',
                # Original fields continue
                'company_name': business_info.get('company_name', '') or '',
                'primary_email': business_info.get('primary_email', '') or '',
                'primary_phone': business_info.get('primary_phone', '') or '',
                'address': business_info.get('address', '') or '',
                'country': business_info.get('country', '') or '',
                'country_confidence': business_info.get('country_confidence', 0) or 0,
                'state_province': business_info.get('state_province', '') or '',
                'city': business_info.get('city', '') or '',
                'local_area': business_info.get('local_area', '') or '',
                'serves_locations': '; '.join(serves_locations) if serves_locations else '',
                'social_media_links': '; '.join([f"{platform}: {url}" for platform, url in social_media.items()]) if social_media else '',
                'website_complexity_score': website_metrics.get('complexity_score', 0) or 0,
                'word_count': website_metrics.get('word_count', 0) or 0,
                'total_links': website_metrics.get('total_links', 0) or 0,
                'has_online_booking': business_info.get('has_online_booking', False),
                'error': result.get('error', '') or '',
                'failed_due_to_connectivity': result.get('failed_due_to_connectivity', False),
                'processed_at': current_time
            }
            
            self.csv_writers['main'].writerow(row)
            self.csv_files['main'].flush()
            
            # Write high-priority targets to separate file
            if result.get('vr_priority') == 'high' and result.get('industry_type') == 'vacation_rental':
                self.csv_writers['high_priority'].writerow(row)
                self.csv_files['high_priority'].flush()
            
            # Add to processed domains
            self.processed_domains.add(result.get('domain', ''))
            
        except Exception as e:
            logger.error(f"Error writing result: {e}")


    def display_live_stats(self):
        """Display live statistics - ENHANCED VERSION"""
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            rate = self.stats['total_processed'] / elapsed if elapsed > 0 else 0
            
            print('\033[2J\033[H', end='')
            print("🔄 LIVE DOMAIN PROCESSING STATISTICS")
            print("=" * 60)
            print(f"📦 Batch: {self.current_batch}/{self.total_batches}")
            print(f"⏱️  Time elapsed: {int(elapsed//60)}m {int(elapsed%60)}s")
            print(f"🚀 Processing rate: {rate:.1f} domains/second")
            print(f"📊 Total processed: {self.stats['total_processed']}")
            print(f"✅ Working: {self.stats['working']} ({self.stats['working']/max(1,self.stats['total_processed'])*100:.1f}%)")
            print(f"🏢 Business: {self.stats['business']} ({self.stats['business']/max(1,self.stats['total_processed'])*100:.1f}%)")
            print(f"🅿️  Parked: {self.stats['parked']} ({self.stats['parked']/max(1,self.stats['total_processed'])*100:.1f}%)")
            print(f"❌ Failed: {self.stats['failed']} ({self.stats['failed']/max(1,self.stats['total_processed'])*100:.1f}%)")
            
            if self.consecutive_failures > 0:
                print(f"🔗 Consecutive failures: {self.consecutive_failures}")
            
            if self.stats['industries']:
                print(f"\n🏷️ TOP INDUSTRIES:")
                sorted_industries = sorted(self.stats['industries'].items(), key=lambda x: x[1], reverse=True)[:5]
                for industry, count in sorted_industries:
                    print(f"   {industry}: {count}")
            
            if self.stats['company_sizes']:
                print(f"\n🏢 COMPANY SIZES:")
                sorted_sizes = sorted(self.stats['company_sizes'].items(), key=lambda x: x[1], reverse=True)
                for size, count in sorted_sizes:
                    size_display = {
                        'large_enterprise': '🏢 Large Enterprise',
                        'medium_business': '🏬 Medium Business', 
                        'small_business': '🏪 Small Business',
                        'unknown': '❓ Unknown'
                    }.get(size, size)
                    print(f"   {size_display}: {count}")
            
            # ENHANCED: Show vacation rental specific stats
            vr_industry_count = self.stats['industries'].get('vacation_rental', 0)
            if vr_industry_count > 0:
                print(f"\n🏖️ VACATION RENTAL ANALYSIS ({vr_industry_count} total):")
                
                # Priority breakdown
                print(f"   🎯 High Priority Targets: {self.stats.get('high_priority_targets', 0)}")
                print(f"   ⭐ Medium Priority Targets: {self.stats.get('medium_priority_targets', 0)}")
                
                # Key metrics
                print(f"   📞 High Decision Maker Access: {self.stats.get('decision_maker_accessible', 0)}")
                print(f"   🔧 Need Website Upgrade: {self.stats.get('website_needs_upgrade', 0)}")
                
                # Business models
                if self.stats.get('vr_business_models'):
                    print(f"\n   📊 Business Models:")
                    for model, count in sorted(self.stats['vr_business_models'].items(), key=lambda x: x[1], reverse=True):
                        model_display = {
                            'direct_owner_small': '🏠 Small Direct Owner (1-5)',
                            'direct_owner_medium': '🏘️ Medium Direct Owner (6-15)',
                            'property_manager_small': '🏢 Small PM (10-50)',
                            'property_manager_medium': '🏬 Medium PM (51-200)',
                            'property_manager_large': '🏭 Large PM (200+)',
                            'listing_platform_small': '📋 Small Directory',
                            'listing_platform_large': '🌐 Large Platform',
                            'software_provider': '💻 Software Provider',
                            'marketing_agency': '📢 Marketing Agency'
                        }.get(model, model)
                        print(f"      {model_display}: {count}")
                
                # Property types
                if self.stats.get('vr_property_types'):
                    print(f"\n   🏖️ Property Types:")
                    for prop_type, count in sorted(self.stats['vr_property_types'].items(), key=lambda x: x[1], reverse=True):
                        print(f"      {prop_type}: {count}")
            
            print(f"\n📝 CSV files are being updated in real-time!")
            print(f"🎯 High-priority targets saved to separate file!")
            print(f"💾 Progress is automatically saved and can be resumed")

    def process_batch(self, batch_domains):
        """Process a batch of domains"""
        batch_results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_domain = {executor.submit(self.check_domain, domain): domain for domain in batch_domains}
            
            for future in as_completed(future_to_domain):
                domain = future_to_domain[future]
                try:
                    result = future.result()
                    self.write_result_realtime(result)
                    batch_results.append(result)
                    
                    # Display progress
                    if self.stats['total_processed'] % 5 == 0:
                        self.display_live_stats()
                    
                    # Enhanced logging with new VR data
                    status = "✓" if result['working'] else "✗"
                    business = "Business" if result.get('is_business', False) else "Parked" if result.get('is_parked', False) else "Other"
                    industry = result.get('industry_type', 'N/A')
                    company_size = result.get('company_size', 'Unknown')
                    
                    # Format company size for display with preference indicators
                    size_emoji = {
                        'large_enterprise': '🏢',
                        'medium_business': '🏬', 
                        'small_business': '🏪⭐',  # Star indicates preferred target
                        'unknown': '❓'
                    }.get(company_size, '❓')
                    
                    # ENHANCED: Better VR logging
                    if result.get('industry_type') == 'vacation_rental':
                        priority = result.get('vr_priority', '')
                        priority_emoji = {'high': '🎯', 'medium': '⭐', 'low': '⚠️'}.get(priority, '')
                        
                        model = result.get('vr_business_model', '')
                        props = result.get('vr_property_count', '')
                        decision_maker = result.get('vr_decision_maker_accessible', '')
                        needs_upgrade = '🔧' if result.get('vr_needs_website_upgrade') else ''
                        
                        logger.info(f"{status} {domain} - VR {priority_emoji} {model} | Props: {props} | DM: {decision_maker} {needs_upgrade}")
                        
                        # Special logging for high-priority targets
                        if priority == 'high':
                            business_info = result.get('business_info', {})
                            logger.info(f"   🎯 HIGH PRIORITY: {business_info.get('company_name', '')[:30]}")
                            logger.info(f"   📞 Contact: {business_info.get('primary_email', '')} | {business_info.get('primary_phone', '')}")
                            logger.info(f"   📍 Location: {business_info.get('city', '')} {business_info.get('country', '')}")
                            logger.info(f"   🎯 Score: {result.get('vr_target_score', 0)} | Factors: {', '.join(result.get('vr_target_factors', [])[:3])}")
                    else:
                        # Original logging for non-VR
                        target_indicator = ""
                        contact_info = ""
                        business_info = result.get('business_info', {})
                        if business_info.get('primary_email') or business_info.get('primary_phone'):
                            contact_info = " 📞"
                        
                        location_info = ""
                        country = business_info.get('country', '')
                        city = business_info.get('city', '')
                        if country:
                            country_flags = {
                                'United States': '🇺🇸',
                                'Canada': '🇨🇦',
                                'United Kingdom': '🇬🇧',
                                'Australia': '🇦🇺',
                                'Germany': '🇩🇪',
                                'France': '🇫🇷',
                                'Spain': '🇪🇸',
                                'Italy': '🇮🇹',
                                'Netherlands': '🇳🇱',
                                'Mexico': '🇲🇽'
                            }
                            flag = country_flags.get(country, '🌍')
                            if city:
                                location_info = f" {flag}({city[:15]})"
                            else:
                                location_info = f" {flag}"
                        
                        website_metrics = business_info.get('website_metrics', {})
                        complexity_score = website_metrics.get('complexity_score', 0)
                        if complexity_score > 25:
                            website_indicator = " 🌐+"
                        elif complexity_score < 10:
                            website_indicator = " 🌐-"
                        else:
                            website_indicator = " 🌐"
                        
                        if result.get('failed_due_to_connectivity', False):
                            logger.warning(f"🔗 {domain} - Connectivity issue")
                        else:
                            logger.info(f"{status} {domain} - {business} ({industry}) {size_emoji}{target_indicator}{contact_info}{location_info}{website_indicator}")
                    
                except Exception as e:
                    logger.error(f"Error checking {domain}: {e}")
        
        return batch_results

    def check_domains_from_list(self, domains, output_dir='.', resume=True):
        """Check domains with batching and resume functionality"""
        if not self.check_network_connectivity():
            print("❌ No internet connectivity detected!")
            if not self.wait_for_connectivity():
                print("❌ Could not establish connectivity. Aborting.")
                return []

        os.makedirs(output_dir, exist_ok=True)

        # Setup progress tracking
        self.setup_progress_tracking(output_dir)
        
        # Load existing progress if resuming
        if resume:
            self.load_progress()
        
        # Filter out already processed domains
        if self.processed_domains:
            remaining_domains = [d for d in domains if d not in self.processed_domains]
            print(f"📋 Resuming: {len(self.processed_domains)} already processed, {len(remaining_domains)} remaining")
        else:
            remaining_domains = domains
            print(f"📋 Starting fresh: {len(remaining_domains)} domains to process")
        
        if not remaining_domains:
            print("✅ All domains already processed!")
            return []
        
        # Setup batching
        total_domains = len(remaining_domains)
        self.total_batches = (total_domains + self.batch_size - 1) // self.batch_size
        self.stats['total_batches'] = self.total_batches
        
        # Setup CSV if not resuming or if it doesn't exist
        self.setup_realtime_csv(output_dir)
        
        if not self.stats['start_time']:
            self.stats['start_time'] = time.time()
        
        print(f"🚀 Processing {total_domains} domains in {self.total_batches} batches of {self.batch_size}")
        print(f"🎯 Enhanced vacation rental classification enabled!")
        
        all_results = []
        
        # Process domains in batches
        for batch_num in range(self.current_batch, self.total_batches):
            self.current_batch = batch_num + 1
            self.stats['current_batch'] = self.current_batch
            
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, total_domains)
            batch_domains = remaining_domains[start_idx:end_idx]
            
            print(f"\n📦 Processing batch {self.current_batch}/{self.total_batches} ({len(batch_domains)} domains)")
            print(f"🔄 Domains: {', '.join(batch_domains[:3])}{'...' if len(batch_domains) > 3 else ''}")
            
            # Check connectivity before each batch
            if not self.check_network_connectivity():
                print(f"⚠️ Network issues detected before batch {self.current_batch}")
                if not self.wait_for_connectivity():
                    print(f"❌ Could not restore connectivity. Stopping at batch {self.current_batch}")
                    break
            
            # Process the batch
            batch_results = self.process_batch(batch_domains)
            all_results.extend(batch_results)
            
            # Save progress after each batch
            self.save_progress()
            
            # Display batch completion
            batch_working = sum(1 for r in batch_results if r.get('working', False))
            batch_business = sum(1 for r in batch_results if r.get('is_business', False))
            batch_high_priority = sum(1 for r in batch_results if r.get('vr_priority') == 'high')
            print(f"✅ Batch {self.current_batch} complete: {batch_working}/{len(batch_domains)} working, {batch_business} business")
            if batch_high_priority > 0:
                print(f"🎯 Found {batch_high_priority} high-priority VR targets in this batch!")
            
            # Brief pause between batches to avoid overwhelming servers
            if self.current_batch < self.total_batches:
                time.sleep(2)
        
        # Final statistics display
        self.display_live_stats()
        
        # Close CSV files
        for file in self.csv_files.values():
            file.close()
        
        print(f"\n✅ Processing complete! Results saved to {output_dir}")
        print(f"📊 Processed {len(all_results)} domains across {self.current_batch} batches")
        
        # Enhanced final summary for VR targets
        high_priority_count = sum(1 for r in all_results if r.get('vr_priority') == 'high')
        medium_priority_count = sum(1 for r in all_results if r.get('vr_priority') == 'medium')
        
        if high_priority_count > 0 or medium_priority_count > 0:
            print(f"\n🎯 VACATION RENTAL LEAD SUMMARY:")
            print(f"   High Priority Targets: {high_priority_count}")
            print(f"   Medium Priority Targets: {medium_priority_count}")
            print(f"   See high_priority_targets CSV for best leads!")
        
        return all_results

def load_domains_from_file(filename):
    """Load domains from text file"""
    try:
        with open(filename, 'r') as f:
            domains = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        logger.info(f"Loaded {len(domains)} domains from {filename}")
        return domains
    except FileNotFoundError:
        logger.error(f"File {filename} not found")
        return []

def create_sample_domains_file():
    """Create a sample domains file for testing"""
    sample_domains = [
        "google.com",
        "facebook.com", 
        "github.com",
        "stackoverflow.com",
        "microsoft.com",
        "amazon.com",
        "netflix.com",
        "airbnb.com",
        "uber.com",
        "spotify.com"
    ]
    
    with open('sample_domains.txt', 'w') as f:
        f.write("# Sample domains for testing\n")
        f.write("# Add your own domains below, one per line\n")
        for domain in sample_domains:
            f.write(f"{domain}\n")
    
    print("📝 Created sample_domains.txt with example domains")

def main():
    """Main function"""
    print("🌐 Domain Checker and Business Identifier - ENHANCED VERSION")
    print("🎯 With Advanced Vacation Rental Lead Detection")
    print("=" * 60)
    
    # Check for domain files
    domain_files = ['domains.txt', 'your_domains.txt', 'sample_domains.txt']
    domains = []
    used_file = None
    
    for filename in domain_files:
        if os.path.exists(filename):
            domains = load_domains_from_file(filename)
            used_file = filename
            break
    
    if not domains:
        print("📁 No domain files found!")
        create_sample = input("Create a sample domains file for testing? (y/n): ").strip().lower()
        if create_sample == 'y':
            create_sample_domains_file()
            domains = load_domains_from_file('sample_domains.txt')
            used_file = 'sample_domains.txt'
        else:
            print("Please create a file named 'domains.txt' with your domains (one per line)")
            return
    
    if not domains:
        print("❌ No valid domains found")
        return
    
    print(f"📊 Loaded {len(domains)} domains from {used_file}")
    
    # Configuration options
    print("\n⚙️ Configuration:")
    batch_size = input("Batch size (default: 50): ").strip()
    batch_size = int(batch_size) if batch_size.isdigit() else 50
    
    max_workers = input("Max concurrent workers (default: 10): ").strip()
    max_workers = int(max_workers) if max_workers.isdigit() else 10
    
    timeout = input("Timeout per domain in seconds (default: 8): ").strip()
    timeout = int(timeout) if timeout.isdigit() else 8
    
    resume = input("Resume from previous progress if available? (y/n, default: y): ").strip().lower()
    resume = resume != 'n'
    
    print(f"\n🚀 Starting with batch_size={batch_size}, workers={max_workers}, timeout={timeout}s")
    print(f"🎯 Enhanced detection will identify:")
    print(f"   - Small to medium vacation rental operators")
    print(f"   - Accessible decision makers")
    print(f"   - Websites needing upgrades")
    print(f"   - Property counts and business models")
    
    # Initialize checker
    checker = DomainChecker(timeout=timeout, max_workers=max_workers, batch_size=batch_size)

    # DEBUG: Print all methods of checker
    print("Available methods in DomainChecker:")
    for method in dir(checker):
        if not method.startswith('_'):
            print(f"  - {method}")
    print("---")

    # Process domains
    results = checker.check_domains_from_list(domains, output_dir='results', resume=resume)
    
    # Final summary
    if results:
        working = sum(1 for r in results if r.get('working', False))
        business = sum(1 for r in results if r.get('is_business', False))
        parked = sum(1 for r in results if r.get('is_parked', False))
        failed = sum(1 for r in results if r.get('error') and not r.get('failed_due_to_connectivity', False))
        connectivity_issues = sum(1 for r in results if r.get('failed_due_to_connectivity', False))
        
        print(f"\n🎉 FINAL SUMMARY")
        print("=" * 30)
        print(f"📊 Total domains: {len(results)}")
        print(f"✅ Working: {working} ({working/len(results)*100:.1f}%)")
        print(f"🏢 Business: {business} ({business/len(results)*100:.1f}%)")
        print(f"🅿️ Parked: {parked} ({parked/len(results)*100:.1f}%)")
        print(f"❌ Failed: {failed} ({failed/len(results)*100:.1f}%)")
        if connectivity_issues > 0:
            print(f"🔗 Connectivity issues: {connectivity_issues} ({connectivity_issues/len(results)*100:.1f}%)")
        
        # ENHANCED: Detailed VR analysis
        vr_results = [r for r in results if r.get('industry_type') == 'vacation_rental' and r.get('working', False)]
        
        if vr_results:
            print(f"\n🏖️ VACATION RENTAL INDUSTRY ANALYSIS ({len(vr_results)} sites)")
            print("=" * 50)
            
            # Priority breakdown
            high_priority = [r for r in vr_results if r.get('vr_priority') == 'high']
            medium_priority = [r for r in vr_results if r.get('vr_priority') == 'medium']
            low_priority = [r for r in vr_results if r.get('vr_priority') == 'low']
            
            print(f"\n📊 PRIORITY BREAKDOWN:")
            print(f"   🎯 High Priority: {len(high_priority)} ({len(high_priority)/len(vr_results)*100:.1f}%)")
            print(f"   ⭐ Medium Priority: {len(medium_priority)} ({len(medium_priority)/len(vr_results)*100:.1f}%)")
            print(f"   ⚠️  Low Priority: {len(low_priority)} ({len(low_priority)/len(vr_results)*100:.1f}%)")
            
            # Business model breakdown
            model_counts = {}
            for r in vr_results:
                model = r.get('vr_business_model', 'unknown')
                model_counts[model] = model_counts.get(model, 0) + 1
            
            print(f"\n📊 BUSINESS MODEL BREAKDOWN:")
            model_names = {
                'direct_owner_small': '🏠 Small Direct Owner (1-5 properties)',
                'direct_owner_medium': '🏘️ Medium Direct Owner (6-15 properties)',
                'property_manager_small': '🏢 Small Property Manager (10-50)',
                'property_manager_medium': '🏬 Medium Property Manager (51-200)',
                'property_manager_large': '🏭 Large Property Manager (200+)',
                'listing_platform_small': '📋 Small Directory/Platform',
                'listing_platform_large': '🌐 Large Platform (Airbnb-style)',
                'software_provider': '💻 B2B Software Provider',
                'marketing_agency': '📢 Marketing/Lead Gen Service',
                'unknown': '❓ Unknown/Unclear'
            }
            
            for model, count in sorted(model_counts.items(), key=lambda x: x[1], reverse=True):
                display_name = model_names.get(model, model)
                print(f"   {display_name}: {count}")
            
            # Key metrics
            accessible_dm = sum(1 for r in vr_results if r.get('vr_decision_maker_accessible') == 'high')
            needs_upgrade = sum(1 for r in vr_results if r.get('vr_needs_website_upgrade') == True)
            has_property_count = sum(1 for r in vr_results if r.get('vr_property_count'))
            
            print(f"\n📊 KEY METRICS:")
            print(f"   📞 High Decision Maker Access: {accessible_dm} ({accessible_dm/len(vr_results)*100:.1f}%)")
            print(f"   🔧 Need Website Upgrade: {needs_upgrade} ({needs_upgrade/len(vr_results)*100:.1f}%)")
            print(f"   📈 Property Count Detected: {has_property_count} ({has_property_count/len(vr_results)*100:.1f}%)")
            
            # Geographic distribution
            geo_scopes = {}
            for r in vr_results:
                scope = r.get('vr_geographic_scope', 'unknown')
                geo_scopes[scope] = geo_scopes.get(scope, 0) + 1
            
            print(f"\n🌍 GEOGRAPHIC SCOPE:")
            for scope, count in sorted(geo_scopes.items(), key=lambda x: x[1], reverse=True):
                print(f"   {scope.title()}: {count}")
            
            # Property types
            prop_types = {}
            for r in vr_results:
                ptype = r.get('vr_property_type', 'unknown')
                prop_types[ptype] = prop_types.get(ptype, 0) + 1
            
            print(f"\n🏖️ PROPERTY TYPES:")
            for ptype, count in sorted(prop_types.items(), key=lambda x: x[1], reverse=True):
                print(f"   {ptype.title()}: {count}")
            
            # Show top high-priority targets
            if high_priority:
                print(f"\n🎯 TOP HIGH-PRIORITY TARGETS:")
                # Sort by target score
                high_priority.sort(key=lambda x: x.get('vr_target_score', 0), reverse=True)
                
                for i, result in enumerate(high_priority[:10], 1):
                    business_info = result.get('business_info', {})
                    print(f"\n   {i}. {result['domain']} (Score: {result.get('vr_target_score', 0)})")
                    print(f"      Model: {result.get('vr_business_model', '')}")
                    print(f"      Properties: {result.get('vr_property_count', 'Unknown')}")
                    print(f"      Decision Maker: {result.get('vr_decision_maker_accessible', '')}")
                    print(f"      Needs Upgrade: {'Yes' if result.get('vr_needs_website_upgrade') else 'No'}")
                    
                    if business_info.get('company_name'):
                        print(f"      Company: {business_info['company_name'][:50]}")
                    if business_info.get('primary_email') or business_info.get('primary_phone'):
                        contact = []
                        if business_info.get('primary_email'):
                            contact.append(business_info['primary_email'])
                        if business_info.get('primary_phone'):
                            contact.append(business_info['primary_phone'])
                        print(f"      Contact: {' | '.join(contact)}")
                    if business_info.get('city') or business_info.get('country'):
                        location = []
                        if business_info.get('city'):
                            location.append(business_info['city'])
                        if business_info.get('country'):
                            location.append(business_info['country'])
                        print(f"      Location: {', '.join(location)}")
                    
                    # Show target factors
                    factors = result.get('vr_target_factors', [])
                    if factors:
                        print(f"      Why High Priority: {'; '.join(factors[:3])}")
        
        print(f"\n💾 All results saved in the 'results' directory")
        print(f"🎯 High-priority targets saved separately for easy access")
        print(f"📈 Use the CSV files to sort, filter, and prioritize your outreach")
        print(f"🚀 Focus on high-priority targets with accessible decision makers!")

if __name__ == "__main__":
    main()
