#!/usr/bin/env python3
"""
step0_map_to_leaf.py

Maps items to tier-3 leaf categories from proposed-subcategories.json.

Architecture:
  1. Tier-1 categorization (same RULES as map_categories_multi.py)
     -> up to MAX_LEAF_CATS tier-1 categories per item (secondary must score >= SECONDARY_THRESHOLD)
  2. For each tier-1 match, apply TIER3_RULES to select the best-scoring tier-3 leaf.
     If no tier-3 rule fires, fall back to the first tier-3 in the first tier-2 of that tier-1.

Output: {env}/output/{timestamp}/items-leaf-mapping.csv
  item_id, item_name, item_description, store_name, tier1, tier2, tier3, category_path

Optional args:
  --input PATH       Override input CSV (default: prod/downloaded/items-with-store-data-2.csv)
  --output PATH      Override output CSV
  --filter-file PATH Path to a text file with one item_id per line; only those items are processed
"""

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent

INPUT_FILE  = str(ROOT / "prod" / "downloaded" / "items-with-store-data-2.csv")
OUTPUT_FILE = str(ROOT / "prod" / "output" / "items-leaf-mapping.csv")
TAXONOMY    = str(ROOT / "proposed-subcategories.json")

SECONDARY_THRESHOLD = 9
MAX_LEAF_CATS       = 3

# ── Tier-1 rules ─────────────────────────────────────────────────────────────
CATEGORIES = [
    "concrete_cement_and_masonry","construction_adhesives_and_sealants","decking",
    "dimensional_lumber_and_composites","doors","drywall_and_accessories","electrical",
    "fencing","flooring","framing_materials","general_fasteners_and_hardware","hvac",
    "insulation","kitchen_and_bath","metals_and_metal_fabrication","miscellaneous",
    "paint_and_stain","plants_and_landscaping","plumbing","rebar_and_reinforcement",
    "roofing_materials","sheathing","siding","sitework_and_drainage",
    "structural_fasteners_and_connectors","timber_logs_and_specialty_wood","tools",
    "weatherproofing_house_wrap","windows",
]

RULES = {
    "fencing": [
        (r"barbed wire",10),(r"field fence",10),(r"cattle panel",10),(r"hog panel",10),
        (r"horse fence",10),(r"non.?climb",10),(r"corral panel",10),(r"mesh gate",10),
        (r"rail gate",10),(r"square corner.*gate|round corner.*gate|5.rail.*gate",10),
        (r"\bt-post\b|t post driver",10),(r"fence stay",10),(r"game fence",10),
        (r"deer.*fence|deer gate|rabbit.*fence",10),(r"poultry.*nett",10),
        (r"split rail",9),(r"vinyl fenc",10),(r"welded wire",7),
        (r"post pounder|post driver",7),(r"\bfenc(e|ing)\b",5),
        (r"\bline post\b|\bcorner post\b|\bblank post\b",8),
    ],
    "timber_logs_and_specialty_wood": [
        (r"hand.?peeled",10),(r"\blogs?\b",7),(r"\bslabs?\b",8),(r"live edge",9),
        (r"natural edge slab",10),(r"glulam",9),(r"reclaimed timber",10),
        (r"post.{0,5}beam",8),(r"\bmantel\b|\bmantle\b",9),(r"lodge.?pole",10),
        (r"hand.?hewn",10),(r"teepee pole",10),(r"\bfirewood\b",9),
        (r"walnut.*slab|black walnut",9),(r"maple.*cookie",10),
        (r"milled.?&.?peeled|milled and peeled",9),(r"\bmilled\b",7),
        (r"\btimbers?\b",7),(r"natural edge",8),(r"heart.?pine",9),(r"\braw\b",5),
        (r"rough.?cut.*lumber|rough.?sawn",7),(r"\bbeam\b",5),
        (r"wood.*slab|timber.*slab",9),
        (r"\bpoplar\b|\bhickory\b|\bwalnut\b|\belm\b|\bash\b|\bcatalpa\b|\bsycamore\b|\bcottonwood\b|\blocust\b|\bcypress\b|\bcherry\b|\bmahogany\b",7),
        (r"sawdust|wood.*chip",6),(r"\bslab\b",8),
        (r"\bwood board\b|\bhardwood board\b|\boak board\b|\bmaple board\b|\bash board\b|\bfull.dimension board\b",9),
        (r"\b\dx\d+x\d+\b",7),(r"carob round|wood round",8),
    ],
    "dimensional_lumber_and_composites": [
        (r"\b[12]x[2-9]\d*\b",9),(r"df #?1|df fohc",9),(r"douglas.?fir",8),
        (r"\bspruce\b",7),(r"kiln.?dried",8),(r"\bs4s\b",9),(r"\bs1s2e\b",9),
        (r"\bs2s\b",9),(r"pressure.?treated",8),(r"treated.*lumber|treated.*board",8),
        (r"dimensional.?lumber",10),(r"\bstud\b",7),(r"df.*select|df select",9),
        (r"\bprecut\b",8),(r"\blumber\b",4),
    ],
    "decking": [
        (r"composite.?deck",10),(r"pvc.?deck",10),(r"deck.?board",10),
        (r"grooved.?board",9),(r"starter.?board",9),(r"picture.?frame.*board",9),
        (r"fascia.*board",8),(r"stair.?riser",9),(r"\bdeck(ing)?\b",7),
        (r"fortress rail",9),(r"timbertech.*rail|\btimbertech\b",9),
        (r"royal guard rail",9),(r"ogden deck",9),(r"cinch rail",9),
        (r"\brail kit\b",7),(r"\bbalusters?\b",8),(r"\bacre.*trim\b",8),
        (r"\bada.*joint\b|\bada.*lineal\b",8),(r"privacy screen system",8),
        (r"\bdck\b",9),(r"\bdeckorators\b",9),
    ],
    "siding": [
        (r"natural edge siding",10),(r"\bsiding\b",9),(r"lap siding",10),
        (r"board.*batten",9),(r"\bt1-11\b",10),(r"hardie",9),
        (r"fiber.?cement.*sid",10),(r"vinyl siding",10),(r"cedar siding",10),
        (r"\bshiplap\b",9),
    ],
    "roofing_materials": [
        (r"\broofing\b",9),(r"\bshingle\b",10),(r"\bunderlayment\b",9),
        (r"ridge.?cap",10),(r"drip.?edge",10),(r"roof.*felt",9),
        (r"roofing.*nail",9),(r"roofing.*flashing",9),
        (r"roof.*coating|silicone.*roof",9),(r"termination.*bar",8),
        (r"torch.*down|torch down",9),(r"app 160",9),(r"modified.*bitumen",9),
    ],
    "sheathing": [
        (r"\bosb\b",9),(r"oriented.?strand.?board",10),(r"\bplywood\b",9),
        (r"\bcdx\b",10),(r"\bsheathing\b",10),
    ],
    "framing_materials": [
        (r"\bjoist\b",9),(r"\brafter\b",9),(r"\bheader\b",8),(r"\btruss\b",9),
        (r"i.?joist",10),(r"\blvl\b",9),(r"laminated.*veneer",9),
        (r"framing.*lumber|framing.*material",10),(r"rim.*joist",9),
        (r"dur.?o.?wall|dur-o-wall",9),
    ],
    "concrete_cement_and_masonry": [
        (r"concrete.*mix",10),(r"\bcement\b",9),(r"\bmortar\b",9),(r"\bgrout\b",8),
        (r"\bbricks?\b",8),(r"concrete.*block|cinder.*block",9),(r"\bpavers?\b",9),
        (r"\bstucco\b",9),(r"\bplaster\b",8),(r"thin.?set|thinset",9),
        (r"\blaticrete\b",9),(r"thin.*brick",9),(r"\bconcrete\b",7),
        (r"\bmasonry\b",7),(r"\bbluestone\b",8),(r"\bflagstone\b",8),
        (r"joint.*sand|polymeric.*sand",8),(r"ledgestone|ledger.*stone",8),
        (r"\bcoping\b",8),(r"retaining.*wall|wall.*block|segmental.*wall",9),
        (r"split.*face.*wall|split.*face",9),(r"\bfirepit\b|\bfire pit\b",7),
        (r"\btravertine\b|\bcobblestone\b|\bcobble\b",8),(r"\bcapstone\b",9),
        (r"\bbelvedere\b|\bmadoc\b|\bkodah\b|\bverona\b",8),
        (r"limestone.*tread|limestone.*cap|limestone.*wall",9),
        (r"\bgeogrid\b|\bgator.*grid\b",8),(r"olde english.*wall|cambridge.*wall|sigma.*wall",9),
        (r"quikrete",8),(r"super.*sand|alliance.*sand",8),
        (r"expansion.*joint",7),(r"anchoring.*cement|anchor.*cement",9),
        (r"bond beam",9),(r"lightweight.*block|split face.*block",9),
        (r"self.?level.*underlayment|self.?level.*floor",8),
    ],
    "rebar_and_reinforcement": [
        (r"\brebar\b",10),(r"steel.*rebar",10),(r"tie.*wire",8),
        (r"reinforc(e|ement|ing)",7),(r"wire.*lath|diamond.*lath",8),
        (r"rebar.*cap|mushroom.*cap.*rebar",9),(r"rebar.*bender|rebar.*cutter",9),
    ],
    "metals_and_metal_fabrication": [
        (r"square.*tube",9),(r"rectangular.*tube|rectangle.*tube",9),
        (r"carbon.*steel",10),(r"steel.*channel|channel.*a36",9),
        (r"angle.*iron",9),(r"steel.*plate|steel.*sheet|1095.*steel.*sheet",9),
        (r"\ba36\b|\ba500\b|\ba513\b",9),(r"\bsteel\b",4),(r"\bmetal\b",4),
        (r"cee.*purlin|zee.*purlin",9),
    ],
    "general_fasteners_and_hardware": [
        (r"\bscrews?\b",7),(r"\bnails?\b",7),(r"\bbolts?\b",7),(r"\bwashers?\b",7),
        (r"\bfastener\b",8),(r"\bhinges?\b",9),(r"cap nail",8),(r"lap screw",9),
        (r"driller screw",9),(r"gator nail",8),(r"spiral.*nail|ring shank",8),
        (r"threaded.*rod|pencil rod",8),(r"\bshackle\b",8),(r"\bchain\b",6),
    ],
    "structural_fasteners_and_connectors": [
        (r"joist.*hanger",10),(r"post.*base|post.*cap",9),(r"beam.*connector",10),
        (r"hurricane.*tie",10),(r"structural.*screw",9),(r"lag.*bolt|lag.*screw",9),
        (r"\bsimpson\b",9),(r"tension.*tie",9),(r"hold.?down",9),
        (r"dur.?o.?wall",9),(r"\ba35\b|\ba23z\b|\bcs16\b|\bls90\b",9),
        (r"\bdtt1z\b|\bbvlz\b|\bh1a\b|\bhuc\d",9),(r"titen",9),
        (r"powder.*actuated|strip.*shot|27 cal",9),
    ],
    "construction_adhesives_and_sealants": [
        (r"\bcaulks?\b",9),(r"\bsealants?\b",9),(r"\badhesive\b",9),
        (r"backer.*rod",9),(r"\bnp.?1\b|\bsonneborn\b|\bmasterseal\b",9),
        (r"wood.*epoxy|liquidwood|woodepox",9),(r"construction.*adhesive",10),
        (r"expanding.*foam|touch.*n.*foam",9),(r"fire.*barrier.*caulk|hilti.*firestop",9),
        (r"\bsilicone\b",7),(r"chinking|log.*home.*caulk",9),
    ],
    "paint_and_stain": [
        (r"sashco.*stain",10),(r"\bstains?\b",8),(r"\bpaints?\b",8),(r"\bprimer\b",9),
        (r"wood.*stain|exterior.*stain",9),(r"masonry.*coating|paint.*coating",9),
        (r"sashco.*preserve|sashco.*prep",9),(r"board.*defense|borate.*preserv",9),
        (r"cap flex|cap.*1g\b",9),(r"\broller\b.*sleeve|\bnap.*roller\b",7),
        (r"chip.*brush|paint.*brush|pro angle.*brush",7),
        (r"paint.*tray|roller.*frame",6),
    ],
    "insulation": [
        (r"\binsulation\b",10),(r"\bbatt\b",9),(r"spray.*foam",9),
        (r"rigid.*foam",9),(r"r.value",9),(r"vapor.*barrier",8),
        (r"foamular",10),(r"sheathall",9),(r"air.*bloc|enviro.*barrier",9),
        (r"touch.*n.*foam|power.*foam|fischer.*foam",8),
        (r"\br.?11\b|\br.?13\b|\br.?19\b|\br.?30\b|\br.?38\b",9),
    ],
    "weatherproofing_house_wrap": [
        (r"house.*wrap",10),(r"\btyvek\b",10),(r"building.*wrap",10),
        (r"flashing.*tape",10),(r"butyl.*tape",9),(r"\bweatherproof",9),
        (r"poly.*sheeting|poly.*sheet",8),(r"\btarps?\b",7),(r"shrink.*wrap",8),
        (r"building.*paper",8),
    ],
    "windows": [
        (r"\bwindows?\b",10),(r"vinyl.*window|retrofit.*window",10),
        (r"glass block",9),(r"window.*sill",8),(r"window.*squeegee",8),
    ],
    "doors": [
        (r"\bdoors?\b",9),(r"barn.*door",10),(r"entry.*door|exterior.*door",10),
        (r"zip.*wall|zipdoor",9),(r"access.*panel",8),(r"mailbox.*door",9),
    ],
    "flooring": [
        (r"\blvp\b",10),(r"luxury.*vinyl.*plank|vinyl.*plank",9),
        (r"hardwood.*floor|laminate.*floor",9),(r"\bflooring\b",8),
        (r"granite.*tile.*floor|floor.*granite.*tile",8),(r"strata.*heat",9),
        (r"kilimanjaro|addis ababa|cairo hickory|zanzibar oak",9),
        (r"red oak.*\d+.*bd ft|white oak.*\d+/\d+.*rough",8),
    ],
    "kitchen_and_bath": [
        (r"\bkitchen\b",8),(r"\bcabinet\b",9),(r"\bcountertop\b",9),
        (r"\bsink\b",8),(r"\bfaucet\b",9),(r"\btoilet\b",9),(r"\bshower\b",8),
        (r"\bvanity\b",9),(r"hydro.?ban",9),(r"\bcoyote\b",8),
        (r"\bmaytrx\b|\bmaytrxren\b",9),(r"oe wall kitchen|oew fa",9),
        (r"fa veneer.*cabinet|faveneer",9),
    ],
    "plumbing": [
        (r"\bplumbing\b",10),(r"pvc.*pipe|copper.*pipe",9),
        (r"drain.*pipe|drainage.*pipe",8),(r"\bvalves?\b",7),(r"pipe.*wrench",8),
        (r"adjustable.*wrench",7),(r"tubing.*level",8),(r"y.connector",8),
    ],
    "electrical": [
        (r"\belectrical\b",10),(r"\bconduit\b",9),(r"\boutlets?\b",9),
        (r"\bromex\b",9),(r"junction.*box",9),(r"electrical.*wire",9),
        (r"\bgfci\b|\bgfi\b",9),(r"power.*strip|surge.*protect",8),
        (r"cep.*power|cep.*triple",8),(r"split.*bolt",8),(r"electrical.*tape",8),
        (r"access.*panel",7),(r"ext.*cord|extension.*cord",6),
    ],
    "hvac": [
        (r"\bhvac\b",10),(r"\bducts?\b",9),(r"\bfurnace\b",9),
        (r"air.*handler",9),(r"heating.*system",8),(r"\bventilation\b",8),
        (r"flex.*duct|mylar.*duct",9),(r"flu.*pipe|flue.*pipe",9),
        (r"duct.*tape",7),(r"weep.*vent|quadro.?vent",9),(r"weeps rect",9),
        (r"bioclimatic.*pergola|fan.*pergola",8),
    ],
    "drywall_and_accessories": [
        (r"\bdrywall\b",10),(r"\bgypsum\b",9),(r"joint.*compound",10),
        (r"corner.*bead",9),(r"drywall.*screw|drywall.*tape",9),
        (r"easy.*sand|diamond.*finish.*plaster",9),(r"dens.*glass",9),
        (r"\bfiberock\b",9),(r"metal.*stud|20.*gauge.*stud",9),
        (r"metal.*track|20.*gauge.*track",9),(r"furring.*channel",8),
        (r"expansion.*bead|double.*v.*bead|j.?bead",8),
    ],
    "sitework_and_drainage": [
        (r"drainage.*pipe|drain.*pipe",9),(r"\bgeotextile\b",9),
        (r"erosion.*control",9),(r"silt.*fence",9),(r"drainage pipe",9),
        (r"gator.*fabric|alliance.*fabric",8),(r"flex.?drain",8),
        (r"geogrid",8),(r"s.*d.*pipe|s&d pipe",9),(r"polyspun",8),
    ],
    "plants_and_landscaping": [
        (r"\blandscap",8),(r"\bgarden\b",7),(r"\bgravel\b",7),
        (r"\blimestone\b",7),(r"wood.*chips?\b",9),(r"\bsand\b",5),
        (r"mesquite|acacia|eucalyptus|pecan|mulberry|citrus.*wood",8),
        (r"pea gravel",9),(r"landscape.*fabric",9),(r"artificial.*grass",9),
        (r"soaker.*hose|garden.*hose",8),(r"hose.*nozzle",7),
    ],
    "tools": [
        (r"\bdrill\b",9),(r"\bsaw\b",8),(r"\bhammers?\b",9),(r"\bwrenches?\b",9),
        (r"\bchisel\b",9),(r"\bgrinder\b",9),(r"\bsander\b",9),
        (r"\bstapler\b",8),(r"\bnailer\b",9),(r"mixing.*paddle",9),
        (r"forstner.*bit",10),(r"tenon.*cutter",10),(r"draw.*knife",10),
        (r"\blumberjack\b",9),(r"caulk.*gun",8),(r"\btrowel\b|\bfresno\b",8),
        (r"mason.*line|chalk.*reel|\bchalk\b",7),
        (r"diamond.*blade|saw.*blade|\bblade\b",8),(r"\blevel\b",7),
        (r"\bscraper\b|\bfloat\b|\bscreed\b",7),
        (r"\bmarshalltown\b|\bstringliner\b|\birwin\b",8),
        (r"\bstihl\b",9),(r"\bbroom\b|\bshovel\b",7),
        (r"\bcup.*wheel\b|\bdiamond.*cup\b|\bdiamond.*grind",8),
        (r"\bsds.*plus\b|\bsds.*bit\b|\bsds.*max\b",8),
        (r"\btape.*measure\b",7),(r"\bscrewdriver\b",7),(r"\bsocket\b",7),
        (r"extension ladder|fiberglass ladder|type 1.a",8),
        (r"cable puller|come.?along",8),(r"ratchet.*strap|bungee",7),
        (r"\btools?\b",4),
    ],
    "miscellaneous": [
        (r"cut.*fee|delivery.*charge|delivery.*fee",7),(r"safety.*product",8),
        (r"rental.*income|state.*tax",7),(r"traffic.*cone|safety.*cone",8),
        (r"\bgloves?\b|\bppe\b|\bharness\b|\blanyard\b",8),
        (r"hard hat|safety.*glass|face.*shield",8),(r"calcium chloride|ice.*melt",8),
        (r"pavilion.*kit|pergola.*kit",7),(r"\bpergola\b",6),
        (r"shop.*vac|contractor.*bag",7),(r"acetone|lacquer.*thinner",7),
        (r"\bwd.?40\b|\bgrease\b",6),
    ],
}

OLD_CAT_HINTS = {
    "lumber_and_composites": ["timber_logs_and_specialty_wood","dimensional_lumber_and_composites","decking","fencing","siding"],
    "building_materials": ["concrete_cement_and_masonry","metals_and_metal_fabrication","rebar_and_reinforcement","roofing_materials"],
    "hardware": ["general_fasteners_and_hardware","tools","structural_fasteners_and_connectors"],
    "tools": ["tools","general_fasteners_and_hardware","decking","fencing"],
    "paint_supplies": ["paint_and_stain","construction_adhesives_and_sealants"],
    "outdoors": ["plants_and_landscaping","fencing","sitework_and_drainage","concrete_cement_and_masonry"],
    "safety": ["miscellaneous"],
    "cleaning_janitorial": ["miscellaneous","construction_adhesives_and_sealants"],
    "flooring": ["flooring"],
    "bath": ["kitchen_and_bath"],"kitchen": ["kitchen_and_bath"],
    "hvac": ["hvac","insulation"],"plumbing": ["plumbing"],"electrical": ["electrical"],
    "decor_and_furniture": ["timber_logs_and_specialty_wood","miscellaneous"],
    "doors_and_windows": ["doors","windows"],"storage_organization": ["miscellaneous"],
}
HINT_BOOST = 2

# ── Tier-3 rules ─────────────────────────────────────────────────────────────
# Format: TIER3_RULES[tier1_slug]["tier2_slug/tier3_slug"] = [(pattern, weight), ...]
TIER3_RULES = {

    "concrete_cement_and_masonry": {
        "concrete_mix_and_bagged_cement/ready_mix_concrete": [
            (r"quikrete",10),(r"sakrete",10),(r"ready.?mix",9),(r"\bconcrete mix\b",9),
            (r"portland.*cement|portland.*concrete",8),(r"bagged.*concrete",8),
        ],
        "concrete_mix_and_bagged_cement/anchoring_cement": [
            (r"anchoring.*cement|anchor.*cement",10),(r"non.?shrink",9),(r"rapidset",8),
        ],
        "concrete_mix_and_bagged_cement/hydraulic_and_specialty_cement": [
            (r"hydraulic.*cement",10),(r"water.?plug|waterplug",10),(r"plug.*cement",8),
        ],
        "mortar_grout_and_thinset/thinset_mortar": [
            (r"\bthinset\b|thin.?set",10),(r"\blaticrete\b",9),
            (r"polymer.*modified.*mortar|fortified.*mortar",8),
        ],
        "mortar_grout_and_thinset/grout": [
            (r"\bgrout\b",10),(r"sanded.*grout|unsanded.*grout|epoxy.*grout",10),
        ],
        "mortar_grout_and_thinset/masonry_mortar": [
            (r"type [snm] mortar|type.s.*mortar|type.n.*mortar",10),
            (r"masonry.*mortar|mortar.*mix|topping.*mix",8),(r"\bmortar\b",6),
        ],
        "mortar_grout_and_thinset/self_leveling_underlayment": [
            (r"self.?level",10),(r"\bunderlayment\b",9),(r"floor.*mud\b",9),
            (r"209 floor",9),(r"\bnxt level\b|nxt.*level",9),(r"\bmasterflo\b",9),
            (r"\bnxt\b.*patch|nxt patch",8),
        ],
        "brick_block_and_cmu/bond_beam_block": [(r"bond beam",10)],
        "brick_block_and_cmu/split_face_block": [(r"split face|split-face",10)],
        "brick_block_and_cmu/lightweight_block": [
            (r"\blightweight\b.*block|light weight.*block",10),(r"\blightweight\b",7),
        ],
        "brick_block_and_cmu/standard_cmu": [
            (r"\bcmu\b",10),(r"concrete.*block|cinder.*block",9),(r"\bhollow block\b",8),
            (r"\bsolid block\b",8),
        ],
        "brick_block_and_cmu/thin_brick": [(r"thin.*brick|brick.*veneer",10)],
        "brick_block_and_cmu/brick": [(r"\bface brick\b|\bcommon brick\b",10),(r"\bbrick\b",5)],
        "concrete_and_stone_pavers/pattern_paver_collections": [
            (r"\bcumberland\b|\bimperial brown\b|\bimperial blue\b",10),
            (r"\bpattern\b.*paver|paver.*\bpattern\b",9),
        ],
        "concrete_and_stone_pavers/travertine_pavers": [(r"\btravertine\b",10)],
        "concrete_and_stone_pavers/limestone_pavers": [
            (r"limestone.*paver|paver.*limestone|limestone.*tread|limestone.*slab",10),
        ],
        "concrete_and_stone_pavers/bluestone_and_flagstone": [
            (r"\bbluestone\b|\bflagstone\b",10),
        ],
        "concrete_and_stone_pavers/concrete_pavers": [
            (r"\bpavers?\b",8),(r"holland paver|plaza slab|diamond paver|weston",9),
            (r"\btumbled\b|\bcobble\b",8),
        ],
        "retaining_wall_systems/geogrid_reinforcement": [
            (r"\bgeogrid\b|5.series.*geogrid|gator.*grid",10),(r"cambridge.*geogrid",9),
        ],
        "retaining_wall_systems/column_kits": [
            (r"\bcolumn kit\b|\bpier cap\b|\bcolumn.*cap\b",10),(r"column light",9),
        ],
        "retaining_wall_systems/corner_and_cap_units": [
            (r"\bcorner.*unit\b|\bcorner.*piece\b|\bcorner.*block\b",9),
            (r"\bwall.*corner\b|\bcorner.*wall\b",8),
        ],
        "retaining_wall_systems/wall_adhesive_and_accessories": [
            (r"wall.*adhesive|block.*adhesive",10),
        ],
        "retaining_wall_systems/wall_blocks": [
            (r"retaining.*wall|wall.*block|segmental.*wall",9),
            (r"\bbelvedere\b|\bmadoc\b|\bkodah\b|\bsigma\b|\bnicolock\b|\bbradstone\b",9),
            (r"\bfullnose\b|\bscarf\b|\bmini colonial\b|\bolde english.*wall\b",9),
            (r"cambridge.*wall|stonehenge wall|verona wall|omega wall|maytrx.*wall",9),
        ],
        "coping_and_capstones/pool_and_wall_coping": [
            (r"camelback coping|streamline coping",10),(r"\bcoping\b",7),
        ],
        "coping_and_capstones/concrete_capstones": [
            (r"\bball cap\b|\bpyramid cap\b|\bpyramid column cap\b",10),
            (r"\bcapstone\b|\bconcrete.*cap\b|\bcolumn cap\b",9),
        ],
        "coping_and_capstones/natural_stone_coping": [
            (r"natural stone coping|bluestone.*coping|limestone.*coping",10),
        ],
        "masonry_veneer_and_thin_products/ledgestone_and_stacked_stone": [
            (r"ledgestone|ledger.*stone|stacked.*stone",10),
        ],
        "masonry_veneer_and_thin_products/travertine_veneer": [
            (r"travertine.*veneer|travertine.*tile",10),
        ],
        "masonry_veneer_and_thin_products/cobblestone_and_cobble_edging": [
            (r"\bcobblestone\b|\bcobble.*edging\b|\bcobble\b",9),
        ],
        "stucco_and_plaster/stucco_base_and_finish": [
            (r"\bstucco\b|\bbase coat\b|\bbrown coat\b|\bscratch coat\b",10),
            (r"\bthoroseal\b",9),
        ],
        "stucco_and_plaster/finish_plaster": [
            (r"finish.*plaster|diamond.*finish|red top|#50.*plaster",10),
            (r"\bplaster\b",6),
        ],
        "stucco_and_plaster/floor_mud": [
            (r"floor.*mud|209.*floor|floor 209|sand.*mix",10),
        ],
        "sand_and_aggregates/masons_sand": [
            (r"mason.?s sand|masonry.*sand|fine.*mason",10),(r"fine sand",7),
        ],
        "sand_and_aggregates/polymeric_sand": [
            (r"polymeric.*sand|joint.*sand|gator.*sand|super.*sand|alliance.*sand",10),
        ],
        "sand_and_aggregates/paver_sand": [
            (r"paver.*sand|bedding.*sand|paver base sand",10),
        ],
        "pigments_and_colorants/lehigh_color_systems": [(r"\blehigh\b",10)],
        "pigments_and_colorants/cement_pigment": [
            (r"cement.*pigment|\bpigment\b|concrete.*color|mortar.*tint|mortar.*color",9),
        ],
        "tile_setting_accessories/tile_spacers_and_wedges": [
            (r"tile.*spacer|1/16.*spacer|3/16.*spacer",10),
        ],
        "tile_setting_accessories/expansion_joint_material": [
            (r"neoprene.*expansion joint|expansion.*joint|expansion joint.*adhesive",10),
        ],
        "tile_setting_accessories/setting_material_and_mastic": [
            (r"\bmastic\b|armor.*bond|tile.*adhesive|setting.*material",10),
        ],
        "firepit_and_outdoor_kitchen_systems/outdoor_kitchen_wall_systems": [
            (r"oe wall.*kitchen|olde english.*wall.*kit|oew.*kitchen",10),
            (r"maytrx.*wall|wall.*kitchen.*kit",9),
        ],
        "firepit_and_outdoor_kitchen_systems/fireplace_kits": [
            (r"fireplace.*kit|\bfireplace\b.*\bkit\b",10),
        ],
        "firepit_and_outdoor_kitchen_systems/firepit_kits": [
            (r"\bfirepit\b|\bfire pit\b|\bbbq kit\b",9),
            (r"berkshire.*firepit|cambridge.*firepit|garden wall.*firepit",9),
            (r"firepit.*kit|\bfirepit\b",8),
        ],
    },

    "tools": {
        "power_tools/drills_and_drivers": [
            (r"\bdrill\b",9),(r"\bdriver\b",8),(r"dewalt.*volt|dewalt.*v\b",9),(r"cordless drill",10),
        ],
        "power_tools/saws": [
            (r"circular saw|recip.*saw|sawzall|jig.*saw|miter saw",10),
        ],
        "power_tools/breakers_and_demolition": [
            (r"\bbreaker\b",9),(r"pavement breaker|demolition breaker",10),
            (r"hilti te|hilti.*te\b",9),(r"imer.*breaker|bosch.*breaker",9),
        ],
        "power_tools/grinders_and_polishers": [
            (r"\bgrinder\b|angle grinder|bench grinder|\bpolisher\b",10),
        ],
        "outdoor_power_equipment/chainsaws": [
            (r"chain saw|chainsaw|\bstihl\b.*saw|saw.*chain\b|chain saw.*c.?be",10),
        ],
        "outdoor_power_equipment/blowers_and_trimmers": [
            (r"\bblower\b|\btrimmer\b|stihl.*blower|stihl.*trimmer",10),
        ],
        "outdoor_power_equipment/ope_accessories": [
            (r"saw chain|bar oil|chainsaw.*oil|guide bar|chain loop",10),
        ],
        "masonry_and_concrete_tools/floats": [
            (r"foam.*float|polyurethane.*float|wood.*float|magnesium.*float|rubber.*float|redwood.*float",10),
            (r"\bfloat\b",6),
        ],
        "masonry_and_concrete_tools/screeds": [
            (r"\bscreed\b|bull.*float|aluminum.*screed",10),
        ],
        "masonry_and_concrete_tools/jointers_and_edgers": [
            (r"\bjointer\b|\bedger\b|\bgroover\b|bricklayer.*jointer",10),
        ],
        "masonry_and_concrete_tools/mixing_tools": [
            (r"mixing.*paddle|mud.*box|mixer.*paddle|drill.*paddle",10),
        ],
        "masonry_and_concrete_tools/trowels": [
            (r"brick trowel|pool trowel|margin trowel|inside corner trowel|finishing trowel|gauging trowel",10),
            (r"\btrowel\b",7),
        ],
        "cutting_tools_and_blades/diamond_cup_wheels": [
            (r"cup wheel|diamond.*cup|grinding.*cup|diamond.*grind",10),
        ],
        "cutting_tools_and_blades/saw_chains_and_accessories": [
            (r"saw chain|chain.*loop|\bchisel chain\b",10),
        ],
        "cutting_tools_and_blades/recip_and_circular_blades": [
            (r"recip.*blade|reciprocating.*blade|circular.*saw.*blade|hacksaw blade",10),
            (r"bosch.*recip.*blade|bosch.*blade",9),
        ],
        "cutting_tools_and_blades/turbo_and_continuous_blades": [
            (r"turbo.*blade|continuous.*rim|turbo rim blade",10),
        ],
        "cutting_tools_and_blades/segmented_diamond_blades": [
            (r"segmented.*diamond.*blade|diamond.*blade",9),
            (r"diamond.*cut.?off|segmented.*blade",8),
        ],
        "measuring_and_layout/tape_measures": [(r"tape.*measure|measuring.*tape",10)],
        "measuring_and_layout/chalk_lines_and_mason_line": [
            (r"\bchalk\b|\bmason.*line\b|\bchalk.*reel\b|\bstringliner\b",9),
        ],
        "measuring_and_layout/squares_and_rafter_tools": [
            (r"rafter square|carpenter square|speed square|\bsquare\b.*tool",9),
            (r"\bempire\b.*square|\bjohnson\b.*square",8),
        ],
        "measuring_and_layout/levels": [
            (r"\blevel\b",7),(r"\btabor\b|\bjohnson\b.*level",8),
        ],
        "hand_tools/specialty_woodworking_tools": [
            (r"draw.*knife|tenon.*cutter|forstner.*bit|\blumberjack\b",10),
        ],
        "hand_tools/wrenches_and_pliers": [
            (r"\bwrench\b|\bpliers\b|pipe.*wrench|adjustable.*wrench",9),
        ],
        "hand_tools/chisels_and_pry_bars": [
            (r"\bchisel\b|\bpry bar\b|\bwrecking bar\b|\bflat bar\b",10),
        ],
        "hand_tools/hammers_and_mallets": [
            (r"brick hammer|drilling hammer|\bhammer\b|\bmallet\b|\bestwing\b",9),
        ],
        "fastening_tools/screw_guns": [
            (r"screw gun|drywall.*screwdriver|drywall driver|dewalt.*heavy duty.*drywall",10),
        ],
        "fastening_tools/nailers": [(r"\bnailer\b",10)],
        "fastening_tools/staplers_and_tackers": [
            (r"\bstapler\b|hammer tacker|sharpshooter|stockade",10),
        ],
        "tool_bits_and_accessories/sds_max_bits": [(r"sds.?max",10)],
        "tool_bits_and_accessories/forstner_and_specialty_bits": [
            (r"forstner|\blumberjack\b.*bit|tenon",10),
        ],
        "tool_bits_and_accessories/bit_holders_and_nut_setters": [
            (r"bit holder|nut setter|hex setter|magnetic.*holder|quick release holder",10),
        ],
        "tool_bits_and_accessories/drill_bits": [
            (r"drill bit|masonry bit|metal.*drill bit|high speed.*bit|\bspade bit\b",9),
        ],
        "tool_bits_and_accessories/sds_plus_bits": [
            (r"sds.?plus|sds\+|\bsds\b.*bit",9),(r"sds.*rebar cut",7),
        ],
        "abrasives_and_surface_prep/wire_wheels_and_grinding_discs": [
            (r"wire wheel|wire brush.*wheel|grinding disc|grinding disk",10),
        ],
        "abrasives_and_surface_prep/drywall_sanding_screens": [
            (r"drywall screen|sanding screen",10),
        ],
        "abrasives_and_surface_prep/sanding_sponges_and_blocks": [
            (r"sanding sponge|sanding block",10),
        ],
        "abrasives_and_surface_prep/sandpaper_sheets": [
            (r"sandpaper|sand paper|sanding.*sheet|wet.*dry.*sand|3m sandpaper",9),
        ],
        "site_and_rigging_tools/ratchet_straps_and_bungees": [
            (r"ratchet.*strap|tie.?down.*strap|\bbungee\b|ratchet.*tie",9),
        ],
        "site_and_rigging_tools/cable_pullers_and_come_alongs": [
            (r"cable puller|come.?along|lever hoist|single line.*cable|double line.*cable",10),
        ],
        "site_and_rigging_tools/ladders": [
            (r"extension ladder|fiberglass ladder|type 1.?a.*ladder|\bladder\b",10),
        ],
        "brooms_and_cleaning_tools/scrapers_and_floor_tools": [
            (r"floor scraper|replacement.*blade.*scraper|\bscraper\b|dasco.*floor|red devil.*floor",9),
        ],
        "brooms_and_cleaning_tools/brushes": [
            (r"pot brush|wire brush|\bbrush\b",7),
        ],
        "brooms_and_cleaning_tools/push_brooms": [
            (r"push broom|street broom|poly broom|tampico broom|\bbroom\b",9),
        ],
        "rental_equipment/mixing_equipment_rentals": [
            (r"mixer.*rental|rental.*mixer",10),
        ],
        "rental_equipment/breaker_rentals": [
            (r"breaker.*rental|rental.*breaker|hilti.*rental|imer.*rental",10),
        ],
    },

    "timber_logs_and_specialty_wood": {
        "hand_peeled_and_milled_logs/raw_logs": [(r"raw log",10)],
        "hand_peeled_and_milled_logs/milled_and_peeled_logs": [
            (r"milled.*peel|milled and peeled",10),
        ],
        "hand_peeled_and_milled_logs/hand_peeled_logs": [
            (r"hand.?peel|hand peeled log",10),
        ],
        "heavy_timbers/spruce_timbers": [(r"spruce.*timber",10)],
        "heavy_timbers/cedar_timbers": [(r"cedar.*timber|cedar.*4x|cedar.*6x",10)],
        "heavy_timbers/douglas_fir_timbers": [
            (r"#2 df [46]\dx[46]\d|#2 df \d+x\d+|df.*4x|df.*6x",9),
            (r"douglas fir.*4x|douglas fir.*6x|douglas fir.*8x|douglas fir.*10x",9),
            (r"\btimbers?\b",6),
        ],
        "slabs_and_live_edge/maple_slabs": [(r"maple.*slab|maple.*cookie|live edge maple",10)],
        "slabs_and_live_edge/oak_slabs": [(r"oak.*slab|live edge oak",10)],
        "slabs_and_live_edge/black_walnut_slabs": [
            (r"walnut.*slab|black walnut|live edge.*walnut",10),
        ],
        "slabs_and_live_edge/mixed_hardwood_slabs": [
            (r"cherry.*slab|elm.*slab|ash.*slab|\blive edge\b|\bslab\b",8),
        ],
        "reclaimed_wood/heart_pine_reclaimed": [(r"heart.?pine|antique.*pine",10)],
        "reclaimed_wood/reclaimed_timbers": [
            (r"reclaimed.*timber|antique.*wood|barn.*wood",10),
        ],
        "glulam_beams/glulam_standard": [(r"glulam|glue.?lam",10)],
        "firewood/firewood_bulk": [
            (r"firewood.*suv.*load|suv.*load.*firewood|truckload.*firewood|firewood.*truckload",10),
            (r"firewood.*cubic|cubic.*firewood|firewood.*pallet|firewood.*variable",9),
        ],
        "firewood/firewood_bundles": [
            (r"firewood bundle|bundle.*firewood|firewood.*3.*pk|firewood 3pk",10),
        ],
        "firewood/firewood_softwood": [
            (r"pine.*firewood|citrus.*firewood|\bcitrus\b.*wood",10),
        ],
        "firewood/firewood_mesquite": [
            (r"mesquite.*firewood|firewood.*mesquite|\bmesquite\b",10),
        ],
        "firewood/firewood_hardwood": [
            (r"\bacacia\b|\bash.*firewood|\belm.*firewood|\beucalyptus.*firewood\b",9),
            (r"\boak.*firewood|\bpecan.*firewood|\balmond.*firewood",9),
            (r"\bfirewood\b",5),
        ],
        "specialty_poles/juniper_and_cedar_posts": [
            (r"juniper.*post|juniper line post|cedar.*post",10),
        ],
        "specialty_poles/teepee_poles": [(r"teepee pole|tipi pole",10)],
        "specialty_poles/lodge_poles": [(r"lodge.?pole",10)],
        "post_and_beam_products/hand_hewn_beams": [(r"hand.?hewn",10)],
        "post_and_beam_products/post_beam_kits": [
            (r"post.*beam kit|timber.*frame kit|post.{0,5}beam",9),
        ],
        "hardwood_boards_and_rough_cut/mixed_hardwood_boards": [
            (r"walnut.*board|maple.*board|ash.*board|oak.*board",9),
        ],
        "hardwood_boards_and_rough_cut/rough_cut_douglas_fir_boards": [
            (r"rough cut|rough.?cut.*df|rough cut douglas|rough.?cut.*fir",9),
        ],
        "hardwood_boards_and_rough_cut/full_dimension_boards": [
            (r"full.?dimension board|full dimension|1x[68].*full|full.dimension",9),
        ],
        "mantels_and_decorative_wood/decorative_slabs_and_rounds": [
            (r"maple.*cookie|carob.*round|wood.*round",10),
        ],
        "mantels_and_decorative_wood/wood_mantels": [
            (r"\bmantel\b|\bmantle\b",10),
        ],
    },

    "dimensional_lumber_and_composites": {
        "kiln_dried_lumber/scaffold_plank": [
            (r"scaffold.*plank|osha.*plank|\bplank\b.*scaffold",10),
        ],
        "kiln_dried_lumber/kd_boards": [(r"kiln.*dry|kiln.?dried",9)],
        "surfaced_and_precut_lumber/precut_studs": [
            (r"precut stud|pre.?cut.*stud|92.?5/8|stud.*92",10),
        ],
        "surfaced_and_precut_lumber/s1s2e_lumber": [(r"\bs1s2e\b",10)],
        "surfaced_and_precut_lumber/s4s_lumber": [(r"\bs4s\b",10)],
        "cedar_dimension_lumber/cedar_framing_sizes": [
            (r"cedar.*2x|cedar.*framing",10),
        ],
        "cedar_dimension_lumber/cedar_boards": [
            (r"cedar.*board|cedar.*1x|\bs1s2e cedar\b",9),
        ],
        "pressure_treated_lumber/pt_above_ground": [
            (r"above.*ground.*treat|above.?ground.*pressure",9),(r"pressure.?treated",5),
        ],
        "pressure_treated_lumber/pt_ground_contact": [
            (r"ground contact|pressure treated.*ground|pt.*ground|green.*treated.*post",9),
        ],
        "spruce_pine_fir/pine_boards": [
            (r"eastern white pine|pine.*board|1x.*pine|white pine",9),
        ],
        "spruce_pine_fir/spf_framing": [
            (r"\bspf\b|hem.?fir|hem fir|spruce.pine.fir",9),
        ],
        "douglas_fir_lumber/df_framing": [
            (r"df.*2x[2-9]|douglas fir.*2x|doug fir.*2x|green.*doug fir|grn.*doug fir",9),
        ],
        "douglas_fir_lumber/df_select_fohc": [
            (r"df.*select|df select|fohc|select.*fohc",10),
        ],
        "douglas_fir_lumber/df_number2_common": [
            (r"#2 df 1x|df.*1x.*board|#2.*df.*board",9),(r"douglas.?fir",5),
        ],
    },

    "decking": {
        "stair_components/stair_rail_kits": [(r"stair.*rail kit|rapid stair kit",10)],
        "stair_components/stair_risers": [(r"stair.*riser|composite.*riser",10)],
        "deck_fasteners_and_hardware/joist_tape": [
            (r"joist tape|deck.*joist.*tape|ogden.*butyl.*joist|butyl.*joist",10),
        ],
        "deck_fasteners_and_hardware/hidden_fasteners": [(r"hidden fastener",10)],
        "deck_fasteners_and_hardware/spiral_deck_nails": [
            (r"spiral.*nail.*deck|gator nail.*spiral|alliance gator nail spiral|gator nail 12",10),
        ],
        "deck_fasteners_and_hardware/composite_deck_screws": [
            (r"composite deck screw|starborn.*deck|starborn.*fascia|fascia.*screw.*starborn|simpson.*deck.*screw|dacro.*ext",9),
        ],
        "deck_gate_kits/rapid_gate_kits": [(r"rapid gate|gate kit|deck.*gate",10)],
        "balusters_and_infill/metal_balusters": [
            (r"metal.*baluster|steel.*baluster|aluminum.*baluster",10),
        ],
        "balusters_and_infill/composite_balusters": [
            (r"composite.*baluster|deckorators.*baluster|classic.*baluster|\bbalusters?\b",9),
        ],
        "deck_railing_systems/ogden_deck_depot_rail": [
            (r"ogden.*deck.*depot|ogden deck",10),
        ],
        "deck_railing_systems/timbertech_rail": [
            (r"timbertech.*rail|timbertech.*fulton|fulton.*rail|\btimbertech\b",10),
        ],
        "deck_railing_systems/royal_guard_rail": [(r"royal guard",10)],
        "deck_railing_systems/fortress_rail": [
            (r"fortress.*rail|fortress.*post|\bfortress\b",10),
        ],
        "deck_railing_systems/cinch_rail": [(r"cinch.*rail|cinch.*ada",10)],
        "decking_trim_and_fascia/ada_lineals_and_joints": [
            (r"\bada\b.*lineal|cinch.*ada|\blineal\b",10),
        ],
        "decking_trim_and_fascia/starter_boards": [(r"starter.*board|starter.*strip",10)],
        "decking_trim_and_fascia/picture_frame_boards": [(r"picture.*frame.*board|picture frame",10)],
        "decking_trim_and_fascia/fascia_boards": [
            (r"fascia.*board|deck.*fascia|vault.*fascia|fascia.*deckorators",9),
        ],
        "decking_trim_and_fascia/acre_trim": [(r"\bacre\b|\bacre trim\b",10)],
        "pvc_decking_boards/pvc_decking_premium": [(r"premium.*pvc.*deck",10)],
        "pvc_decking_boards/pvc_decking_standard": [(r"pvc.*deck.*board|cellular pvc deck",9)],
        "composite_decking_boards/deckorators_pioneer": [
            (r"\bpioneer\b.*deck|deck.*\bpioneer\b",10),
        ],
        "composite_decking_boards/deckorators_vista_voyage": [
            (r"\bvista\b.*deck|deck.*\bvista\b|\bvoyage\b.*deck|deck.*\bvoyage\b",10),
            (r"dck.*vista|dck.*voyage",9),
        ],
        "composite_decking_boards/deckorators_summit_vault_venture": [
            (r"\bsummit\b.*deck|deck.*\bsummit\b|\bvault\b.*deck|deck.*\bvault\b|\bventure\b.*deck",10),
            (r"dck.*summit|dck.*vault|dck.*venture",9),
        ],
        "composite_decking_boards/deckorators_infinity": [
            (r"\binfinity\b.*deck|deck.*\binfinity\b|dck.*infinity",10),
            (r"concrete grey|oasis palm|tiger cove|caribbean coral",8),
        ],
        "composite_decking_boards/deckorators_apex": [
            (r"\bapex\b.*deck|deck.*\bapex\b|dck.*apex|dck - apex",9),
        ],
    },

    "fencing": {
        "fencing_tools_and_accessories/fence_stays_and_clips": [
            (r"fence stay|fence clip",10),
        ],
        "fencing_tools_and_accessories/post_pounders": [
            (r"post pounder|post driver|t.post.*pounder|t-post.*pound",10),
        ],
        "fence_posts_and_stakes/t_posts": [(r"\bt.?post\b",10)],
        "fence_posts_and_stakes/treated_wood_posts": [
            (r"treated.*wood.*post|wood post|treated.*post",9),
        ],
        "fence_gates/cedar_rail_gates": [
            (r"\b2 rail.*gate\b|\b3 rail.*gate\b|cedar.*rail.*gate|rail gate",9),
        ],
        "fence_gates/five_rail_gates": [(r"5.?rail.*gate|five rail.*gate",10)],
        "fence_gates/square_corner_mesh_gates": [
            (r"square corner.*gate|square.*mesh gate",10),
        ],
        "fence_gates/round_corner_mesh_gates": [
            (r"round corner.*gate|round.*mesh gate",10),
        ],
        "split_rail_fencing/split_rail_posts": [
            (r"\bline post\b|\bcorner post\b|\bend post\b|blank post|split rail.*post",9),
        ],
        "split_rail_fencing/split_rails": [
            (r"split rail|jumbo split rail|standard split rail|blank.*split rail",10),
        ],
        "livestock_panels/corral_panels_mesh_filled": [
            (r"corral.*mesh|mesh.*corral|mesh filled.*corral",10),
        ],
        "livestock_panels/corral_panels_flat_iron": [
            (r"\bcorral.*panel\b|flat.*corral|5.?rail.*corral",9),
        ],
        "livestock_panels/hog_panels": [(r"hog panel",10)],
        "livestock_panels/cattle_panels": [(r"cattle panel",10)],
        "welded_wire_fencing/deer_and_rabbit_fence": [
            (r"deer.*rabbit|rabbit.*fence|deer.*fence|deer.*mesh|deer.*gate",10),
        ],
        "welded_wire_fencing/welded_wire_galvanized": [
            (r"welded wire|galv.*welded|welded.*galv",9),
        ],
        "horse_and_non_climb_fencing/non_climb_black": [
            (r"non.?climb.*black|black.*non.?climb",10),
        ],
        "horse_and_non_climb_fencing/non_climb_galvanized": [
            (r"non.?climb|1348|1660|1972",10),
        ],
        "wire_fencing/high_tensile_solid_lock": [
            (r"high tensile|solid lock|1775|2096",10),
        ],
        "wire_fencing/field_fence": [(r"field fence|832.6|939.6|847.6",10)],
        "wire_fencing/barbed_wire": [(r"barbed wire|barb.?wire",10)],
    },

    "metals_and_metal_fabrication": {
        "metal_accessories_and_fittings/cee_and_zee_purlin": [
            (r"cee purlin|zee purlin|c.?ee.*purlin|\bcee\b.*purlin",10),
        ],
        "metal_accessories_and_fittings/end_caps_and_plugs": [
            (r"\bend cap\b|\bsquare cap\b|\bdomed cap\b|\btube cap\b|1 7/8.*cap",9),
        ],
        "steel_pipe/sch80_pipe": [(r"sch.?80|schedule 80",10)],
        "steel_pipe/sch40_pipe": [(r"sch.?40|schedule 40|sch40",9),(r"\ba53\b",7)],
        "steel_sheet_and_plate/steel_plate": [(r"steel plate",10)],
        "steel_sheet_and_plate/carbon_steel_sheet_1095": [
            (r"carbon steel sheet|1095.*sheet|1095.*carbon|\.188.*carbon",10),
        ],
        "structural_channel_and_angle_iron/steel_channel": [
            (r"steel channel|channel.*a36|\bchannel\b.*steel",10),
        ],
        "structural_channel_and_angle_iron/angle_iron": [
            (r"angle iron|\bangle.*iron\b",10),
        ],
        "rectangular_tubing/rectangular_tubing": [
            (r"rectangular.*tube|rectangle.*tube",8),
        ],
        "square_tubing/square_tubing": [
            (r"square.*tube",8),
        ],
    },

    "general_fasteners_and_hardware": {
        "abrasives_and_surface_prep/drywall_screens": [
            (r"drywall screen|sanding screen",10),
        ],
        "abrasives_and_surface_prep/sanding_sponges_and_blocks": [
            (r"sanding sponge|sanding block",10),
        ],
        "abrasives_and_surface_prep/sandpaper": [
            (r"sandpaper|sand paper|3m sandpaper|sanding sheet",9),
        ],
        "chain_and_rigging/shackles_and_connectors": [
            (r"\bshackle\b|screw pin.*shackle|rigging.*connector",10),
        ],
        "chain_and_rigging/chain": [(r"\bchain\b.*per foot|\bchain\b",7)],
        "hinges_and_latches/gate_hinges": [(r"gate hinge|gate.*hinge|\bhinges?\b",9)],
        "bolts_nuts_and_threaded_rod/threaded_rod": [
            (r"threaded rod|pencil rod",10),
        ],
        "bolts_nuts_and_threaded_rod/nuts_and_washers": [
            (r"\bwasher\b|\bhex nut\b|\bnuts?\b",8),
        ],
        "bolts_nuts_and_threaded_rod/hex_head_bolts": [
            (r"hex head bolt|tap bolt|hex.*bolt",9),
        ],
        "nails_and_spikes/spiral_and_ring_shank_nails": [
            (r"gator nail|alliance gator nail|spiral.*nail|ring shank",10),
        ],
        "nails_and_spikes/cap_nails": [(r"cap nail\b|cap nails\b",10)],
        "nails_and_spikes/duplex_and_double_head_nails": [
            (r"duplex nail|double head nail|\d+d duplex",10),
        ],
        "nails_and_spikes/common_nails": [
            (r"common nail|\b\d+d common\b|6d.*nail|8d.*nail|16d.*nail|60d.*nail",9),
        ],
        "screws/coarse_thread_screws": [(r"coarse.*thread.*screw",8)],
        "screws/wood_and_sheet_metal_screws": [
            (r"#9.*screw|wood.*screw|sheet metal.*screw|wafer head screw",9),
        ],
        "screws/panel_and_lap_screws": [
            (r"driller screw|lap screw|panel screw",10),
        ],
    },

    "paint_and_stain": {
        "paint_applicator_supplies/paint_trays_and_accessories": [
            (r"paint tray|roller tray|plastic paint tray",10),
        ],
        "paint_applicator_supplies/paint_brushes_and_chips": [
            (r"chip brush|paint brush|pro angle.*brush|super silk.*brush|asev.*brush|microfiber.*brush",9),
        ],
        "paint_applicator_supplies/roller_frames": [
            (r"roller frame|\broller.*frame\b",10),
        ],
        "paint_applicator_supplies/mini_roller_sleeves": [
            (r"mini.*roller sleeve|4.*roller sleeve|4.*nap roller|mini.*nap",10),
        ],
        "paint_applicator_supplies/roller_sleeves": [
            (r"9.*roller sleeve|9.*nap.*roller|9.*paint.*sleeve|roller sleeve|nap.*roller|roller.*sleeve",9),
        ],
        "primers/wood_primers": [(r"wood.*primer|sealcoat.*primer",10)],
        "primers/masonry_primers": [(r"masonry.*primer|block.*primer",10)],
        "exterior_paint/jobsite_paint": [
            (r"jobsite.*paint|paint.*jobsite|hunter green.*paint",10),
        ],
        "masonry_and_concrete_coatings/self_leveling_primer": [
            (r"cp prime|self.?leveling.*prim|belter.*prime|belter.*tech.*prime",10),
        ],
        "masonry_and_concrete_coatings/elastomeric_coatings": [
            (r"thorocoat|elastomeric|basf.*thoro",10),
        ],
        "masonry_and_concrete_coatings/heavy_duty_masonry_paint": [
            (r"heavy duty masonry coating|masonry.*coating.*quickrete|quickrete.*coat",10),
        ],
        "wood_preservatives_and_prep/sashco_preserve_and_prep": [
            (r"sashco.*preserve|sashco.*prep|sashco.*stretch",10),
        ],
        "wood_preservatives_and_prep/borate_preservatives": [
            (r"board defense|borate|timbor|bora.?care",10),
        ],
        "wood_stains_and_finishes/penetrating_oil_finishes": [
            (r"penetrating.*oil|danish oil|teak oil|linseed",10),
        ],
        "wood_stains_and_finishes/exterior_wood_stains": [
            (r"exterior.*stain|deck.*stain|sashco.*wood",9),
        ],
        "wood_stains_and_finishes/log_and_timber_stains": [
            (r"cap flex|cap.*flex|\bcap \w+ 1g\b|sashco.*stain|log.*stain",10),
            (r"\bcap\b.*(chestnut|driftwood|mahogany|autumn|hazelnut|sequoia|wheat|natural|cedar|walnut|pine)",9),
        ],
    },

    "construction_adhesives_and_sealants": {
        "epoxy_anchor_systems/construction_adhesives_general": [
            (r"construction adhesive|3m.*high strength.*contact|premium mastic|contact adhesive|3m.*77",10),
        ],
        "epoxy_anchor_systems/adhesive_anchor_capsules": [
            (r"epoxy.*anchor|hilti.*epoxy|epoxy.*sleeve",10),
        ],
        "expanding_foam/fill_and_seal_foam": [
            (r"fill.*seal.*foam|fill and seal|powers.*foam|power foam|fischer.*foam|gorilla.*spray|fast.*bonding foam",10),
            (r"touch.*n.*foam",9),
        ],
        "expanding_foam/gaps_and_cracks_foam": [
            (r"gaps.*cracks|gaps and cracks|dow.*gaps|minimal.*foam",10),
        ],
        "wood_epoxy_and_fillers/epoxy_pigments": [
            (r"epoxy.*pigment|woodepox.*pigment",10),
        ],
        "wood_epoxy_and_fillers/woodepox_filler": [
            (r"woodepox|wood.*epox|aba.*woodepox",10),
        ],
        "wood_epoxy_and_fillers/liquidwood_consolidant": [
            (r"liquidwood|liquid.*wood|aba.*liquidwood",10),
        ],
        "backer_rod_and_foam_backer/flat_backer_strip": [
            (r"grip strip|sashco.*grip|flat backer",10),
        ],
        "backer_rod_and_foam_backer/round_backer_rod": [
            (r"backer rod|backer.*rod|\bbacker\b.*\d+.*ft|\broll.*backer|backer.*roll",9),
        ],
        "caulks/fire_barrier_caulks": [
            (r"fire barrier|3m.*fire.*barrier|hilti.*firestop|cp 25wb",10),
        ],
        "caulks/general_caulks": [
            (r"dap alex plus|red devil.*caulk|alex.*plus|ge.*caulk\b|general.*caulk",9),
        ],
        "caulks/sashco_caulk": [(r"sashco.*caulk|sashco.*sealant|\bsashco\b",9)],
        "caulks/log_home_chinking_caulk": [
            (r"\bcon\b.*(brown tone|frontier gold|harvest wheat|weathered gray|grizzly brown|red tone)",10),
            (r"chinking|log home.*caulk",9),
        ],
        "silicone_sealants/silicone_colored": [
            (r"silicone.*white|silicone.*bronze|silicone.*black|colored.*silicone",9),
        ],
        "silicone_sealants/silicone_clear": [
            (r"silicone.*clear|clear.*silicone|100.*silicone.*clear",9),(r"\bsilicone\b",5),
        ],
        "polyurethane_sealants/non_sag_sealants": [
            (r"non.?sag sealant|non.?sag.*polyurethane",10),
        ],
        "polyurethane_sealants/self_leveling_sealants": [
            (r"self.?leveling sealant|sikaflex.*sl|self.?leveling.*joint",10),
        ],
        "polyurethane_sealants/np1_and_sonneborn": [
            (r"\bnp.?1\b|sonneborn|masterseal.*np|masterseal.*sealant|masterseal.*125|masterseal.*clear",10),
        ],
    },

    "drywall_and_accessories": {
        "drywall_screws_and_fasteners/cement_board_screws": [
            (r"cement board screw",10),
        ],
        "drywall_screws_and_fasteners/fine_thread_drywall_screws": [
            (r"fine thread.*drywall|fine.*drywall screw",10),
        ],
        "drywall_screws_and_fasteners/coarse_thread_drywall_screws": [
            (r"coarse thread.*drywall|coarse.*drywall screw",9),(r"drywall.*screw",6),
        ],
        "tape_corner_bead_and_trim/mesh_and_paper_tape": [
            (r"drywall tape|mesh.*tape|joint tape|fiba fuse|paperless.*tape|g.force.*tape",10),
        ],
        "tape_corner_bead_and_trim/expansion_bead": [
            (r"double v.*bead|expansion bead|j.?bead|exp.*bead|mini bead",10),
        ],
        "tape_corner_bead_and_trim/metal_corner_bead": [
            (r"corner bead|1-a corner bead|metal.*corner bead",10),
        ],
        "joint_compound_and_finishing/finish_plaster_and_diamond": [
            (r"diamond finish plaster|finish.*plaster|red top.*plaster",10),
        ],
        "joint_compound_and_finishing/fast_setting_compound": [
            (r"easy sand|setting.*compound|hot mud|joint.*compound",9),
        ],
        "metal_framing/metal_framing_accessories": [
            (r"furring channel|furring.*clip|angle.*20 gauge|\bfurring\b",9),
        ],
        "metal_framing/metal_track": [
            (r"metal track|20 gauge track|light gauge track|\d+ gauge.*track",10),
        ],
        "metal_framing/metal_studs": [
            (r"metal stud|20 gauge stud|light gauge stud|\d+ gauge.*stud",10),
        ],
        "drywall_sheets_and_panels/cement_board": [
            (r"\bfiberock\b|\bcement board\b|\bdurock\b|\bhardibacker\b",10),
        ],
        "drywall_sheets_and_panels/glass_mat_sheathing": [
            (r"dens.*glass|glass mat|glass.*sheathing",10),
        ],
        "drywall_sheets_and_panels/standard_gypsum_board": [
            (r"gypsum board|\bdrywall\b.*board|board.*\bdrywall\b|\bsheetrock\b",9),
        ],
    },

    "rebar_and_reinforcement": {
        "rebar_accessories/rebar_caps": [
            (r"rebar cap|mushroom.*rebar cap|osha.*rebar cap|rebar cap.*#[0-9]|osha.*re.?bar cap",10),
        ],
        "rebar_accessories/tie_wire_and_twisters": [
            (r"tie wire|wire tie|loop.*wire tie|kraft.*tie wire|tie wire twister",10),
        ],
        "rebar_accessories/rebar_benders_and_cutters": [
            (r"rebar bender|rebar cutter|rebar.*bender.*cutter|sds.*rebar cut|ivy.*rebar|hit.*rebar|multiquip.*rebar",10),
        ],
        "wire_mesh_and_lath/diamond_wire_lath": [
            (r"diamond wire lath|wire lath|\blath\b",10),
        ],
        "wire_mesh_and_lath/concrete_wire_mesh": [
            (r"concrete wire|wire mesh|4x4.*gauge.*epoxy|wire mesh.*epoxy|concrete.*wire",10),
        ],
        "rebar/epoxy_coated_rebar": [
            (r"epoxy coated rebar|epoxy.*rebar|epoxy.*#[0-9]|rebar.*epoxy",10),
        ],
        "rebar/rebar": [(r"\brebar\b|re-rod|#[3-9] rebar",8)],
    },

    "roofing_materials": {
        "roofing_fasteners_and_tools/rafter_squares": [
            (r"rafter square|carpenter square|johnson.*square|empire.*square|johnson.*7.*square",10),
        ],
        "roofing_fasteners_and_tools/shingle_shovels": [
            (r"shingle shovel|shingle remover|strip.*fast|razorback.*shovel|roof zone.*shovel",10),
        ],
        "roofing_fasteners_and_tools/roofing_nails": [
            (r"roofing nail|roof.*cap nail|\bgrip rite.*1.*galv.*roof|galvanized.*roofing nail",10),
        ],
        "flashing_and_termination/pvc_flashing": [
            (r"pvc.*flashing|\bpvc\b.*\d+\".*150",10),
        ],
        "flashing_and_termination/termination_bars": [(r"termination bar",10)],
        "flashing_and_termination/metal_drip_edge": [
            (r"drip edge|stainless.*drip",10),
        ],
        "flashing_and_termination/peel_and_stick_flashing": [
            (r"peel.*stick.*flash|sand.?o.?seal|textroflash|hohman.*barnard.*flash",10),
        ],
        "roof_coatings_and_sealants/emulsion_and_tar_coatings": [
            (r"karnak.*920|karnak.*emulsion|flashing cement|modified bitumen.*adhesive|trowel grade.*adhesive|karnak.*66",10),
        ],
        "roof_coatings_and_sealants/silicone_roof_coatings": [
            (r"silicone.*roof.*coating|gaco.*gr.*|silicone roof coating|\bgaco\b",9),
        ],
        "roof_coatings_and_sealants/aluminum_roof_coatings": [
            (r"aluminum.*roof.*coating|roof.*coating.*aluminum|fibered.*aluminum|non.?fibered.*aluminum",10),
            (r"bulldog.*roof|karnak.*roof|del.?val.*roof",9),
        ],
        "shingles_and_underlayment/roof_fabric": [
            (r"roof fabric|adfors.*roof|roof.*fabric",10),
        ],
        "shingles_and_underlayment/roof_felt_and_paper": [
            (r"roof paper|roof felt|15lb.*roof|roof.*15lb|30lb.*roof|roof.*30lb|90lb.*roof|roof.*90lb",10),
        ],
        "shingles_and_underlayment/modified_bitumen_torch_down": [
            (r"torch down|modified bitumen|app 160|torch.*smooth|torch.*sand",10),
            (r"ruberoid|pluvitec|johns manville.*torch|nord.*app|firestone.*torch|valuweld",9),
        ],
    },

    "insulation": {
        "vapor_and_moisture_barriers/liquid_applied_air_barrier": [
            (r"air bloc|henry.*air bloc|enviro barrier|air.*vapor barrier.*liquid|fluid.?applied.*barrier",10),
        ],
        "spray_foam/foam_applicator_systems": [
            (r"touch n foam.*applicator|foam.*applicator gun|eco applicator gun",10),
        ],
        "spray_foam/fill_seal_foam": [
            (r"fill.*seal|fill and seal|powers.*foam|power foam|fischer.*foam|gorilla.*spray|fast.*bonding foam",10),
        ],
        "spray_foam/gaps_cracks_foam": [
            (r"gaps.*cracks|gaps and cracks|dow.*20.*oz.*gaps|\bgaps\b.*cracks",10),
        ],
        "rigid_foam_board/poly_blue_foam_sheathing": [
            (r"sheathall|poly blue.*sheathall|sheathall 2lb",10),
        ],
        "rigid_foam_board/xps_rigid_foam": [
            (r"foamular",9),(r"xps.*foam|rigid.*foam.*board",8),
        ],
        "batt_and_roll_insulation/insulation_batts": [
            (r"\binsulation\b|\bbatt\b|\br.?[0-9]+\b.*insulation|foil.*insulation|foil faced",6),
        ],
    },

    "weatherproofing_house_wrap": {
        "shrink_wrap/shrink_wrap_rolls": [(r"shrink wrap",10)],
        "tarps_and_temporary_covers/fire_retardant_tarps": [
            (r"fire retardant tarp|fr.*tarp|fire.*retardant.*tarp|boen.*fire",10),
        ],
        "tarps_and_temporary_covers/green_heavy_duty_tarps": [
            (r"green.*tarp|green.*hd.*tarp|green.*heavy.*duty|boen.*green|g.force.*green",9),
        ],
        "tarps_and_temporary_covers/blue_poly_tarps": [
            (r"blue tarp|blue.*poly.*tarp|\bblue\b.*tarp",9),(r"\btarp\b",5),
        ],
        "vapor_barriers_and_poly_sheeting/string_reinforced_poly": [
            (r"string reinforced poly|string.*reinforced.*sheeting|nfr.*poly|poly.*nfr|jaydee.*poly",10),
        ],
        "vapor_barriers_and_poly_sheeting/poly_sheeting": [
            (r"poly.*sheeting|\bmil.*poly|\bpoly.*mil\b|ultrasac.*plastic|g.force.*plastic",7),
        ],
        "flashing_tape_and_sealing_tape/deck_joist_tape": [
            (r"joist tape|ogden.*deck.*butyl|deck joist tape|butyl.*joist",10),
        ],
        "flashing_tape_and_sealing_tape/butyl_tape": [
            (r"butyl tape|single bead butyl|butyl.*sealant.*tape",10),
        ],
        "house_wrap_and_building_wrap/building_paper": [
            (r"reinforced building paper|building paper|g.force.*48.*building|48.*building paper|heavy duty.*building",9),
        ],
        "house_wrap_and_building_wrap/tyvek_equivalent": [
            (r"tyvek|house wrap|building wrap",10),
        ],
    },

    "structural_fasteners_and_connectors": {
        "structural_screws_and_bolts/powder_actuated_fasteners": [
            (r"powder.*actuated|powder shot|27 cal.*shot|strip shot|pin.*washer.*100 pc",10),
        ],
        "structural_screws_and_bolts/titen_concrete_anchors": [
            (r"\btiten\b|hd titen|concrete anchor.*titen",10),
        ],
        "structural_screws_and_bolts/lag_screws": [
            (r"lag screw|starborn.*lag|starborn.*3/8.*x|lag.*3/8",9),
        ],
        "structural_screws_and_bolts/sds_structural_screws": [
            (r"sds screw|simpson.*sds.*screw|1/4.*x.*4.*sds.*screw",10),
        ],
        "hurricane_and_seismic_straps/continuous_strap": [
            (r"\bcs16\b|\ba35\b|\ba23z\b|\bls90\b|\blscz\b|continuous strap|framing angle.*simpson|simpson.*a35|simpson.*a23",10),
        ],
        "hurricane_and_seismic_straps/tension_and_tiedown": [
            (r"\bdtt1z\b|\bbvlz\b|tension tie|tiedown connector|simpson.*dtt|simpson.*bvlz",10),
        ],
        "hurricane_and_seismic_straps/hurricane_ties": [
            (r"hurricane tie|\bh1a\b|simpson.*h1a",10),
        ],
        "post_bases_and_caps/deck_rail_post_hardware": [
            (r"deck.*post.*base|fortress.*post|timbertech.*post|royal guard.*post|juniper.*post|post.*skirt.*cap|ic.*post",9),
        ],
        "post_bases_and_caps/post_bases": [
            (r"\bbc[46]\b|\bac[46]\b|\babw[46][46]z\b|post base|column base|simpson.*bc|simpson.*ac|simpson.*abw",10),
        ],
        "joist_hangers_and_beam_connectors/masonry_wall_reinforcement": [
            (r"dur.?o.?wall|dur-o-wall|truss.*masonry|masonry.*truss.*wire|durawall",10),
        ],
        "joist_hangers_and_beam_connectors/joist_hangers": [
            (r"joist hanger|\bhuc\d|simpson.*huc|x.*joist.*hanger",10),
        ],
    },

    "sitework_and_drainage": {
        "geogrid_and_ground_stabilization/gator_grid": [(r"gator.*grid|\bgator grid\b",10)],
        "geogrid_and_ground_stabilization/biaxial_geogrid": [
            (r"cambridge.*geogrid|cambridge.*5.series|5-series.*geogrid|geogrid.*100",10),
        ],
        "erosion_control/silt_fence": [(r"silt fence",10)],
        "geotextile_fabric/spun_bond_geotextile": [
            (r"gator fabric 3 spun|gator fabric 20 poly|polyspun|spun.?bond|gator.*polyspun",10),
            (r"gator fabric 20",9),
        ],
        "geotextile_fabric/non_woven_geotextile": [
            (r"gator fabric 3\.5 non.?woven|gator fabric 4\.4 non.?woven|alliance.*non.?woven|non.woven.*gator",10),
            (r"non.woven|3\.5 non|4\.4 non",9),
        ],
        "geotextile_fabric/woven_geotextile": [
            (r"gator fabric 5 woven|gator fabric.*woven|woven.*geotextile",10),
            (r"gator fabric 5",9),
        ],
        "drainage_pipe_and_fittings/downspout_adapters": [
            (r"downspout adapter|flex.?drain.*downspout|downspout.*adapter",10),
        ],
        "drainage_pipe_and_fittings/pvc_fittings": [
            (r"pvc.*tee|normandy.*tee|4.*tee.*pvc|pvc.*fitting",10),
        ],
        "drainage_pipe_and_fittings/perforated_drain_pipe": [
            (r"perforated.*drain|corrugated.*perforated|s.*d.*pipe.*perf|perf.*pipe",10),
        ],
        "drainage_pipe_and_fittings/solid_flexible_drain_pipe": [
            (r"solid.*flexible.*drain|flex.?drain.*solid|flex-drain.*solid|flex-drain.*\d+.*solid|corrugated.*pipe\b",9),
        ],
    },

    "plants_and_landscaping": {
        "irrigation_and_hoses/hose_nozzles_and_fittings": [
            (r"hose nozzle|pattern.*nozzle|metal.*nozzle|brass.*nozzle|pistol grip.*nozzle|hose.*coupling|female.*coupling|hose.*connector",9),
        ],
        "irrigation_and_hoses/soaker_hoses": [(r"soaker hose|soaker.*hose",10)],
        "irrigation_and_hoses/garden_hoses": [
            (r"flexon.*hose|flexon.*\d+|garden hose|heavy duty.*hose|\d+.*light duty hose|\d+.*heavy duty hose",9),
        ],
        "landscape_fabric_and_ground_cover/artificial_grass": [
            (r"artificial grass|garden mark.*grass|artificial turf|garden mark.*(kentucky|montana|augusta)",10),
        ],
        "landscape_fabric_and_ground_cover/woven_landscape_fabric": [
            (r"craftsmen.*landscape fabric|landscape fabric|craftsmen.*3.*50",10),
        ],
        "landscape_stone/cobblestone_edging": [
            (r"cobblestone edging|victorian boarder|catalina grana|dutch.*cobbled.*limestone",10),
        ],
        "landscape_stone/french_limestone_tile": [
            (r"french limestone|2cm.*limestone|2 cm.*limestone",10),
        ],
        "landscape_stone/limestone_treads_and_caps": [
            (r"limestone tread|limestone.*cap\b|limestone.*wall",9),
        ],
        "sand/bulk_masons_sand": [
            (r"mason.*sand.*super sack|bulk.*mason.*sand|super sack.*sand|1 super sack.*sand",10),
        ],
        "sand/bagged_sand": [
            (r"bagged sand|50.*lb.*sand.*bag|bagged.*sand.*50|50lb.*sand|\bsand\b",5),
        ],
        "gravel_and_aggregate/crushed_stone": [
            (r"crushed stone|3/4.*stone.*bag|bagged.*stone|bag.*3/4 stone|1 ton bag|1.ton.*bag",9),
        ],
        "gravel_and_aggregate/pea_gravel": [
            (r"pea gravel|\d+.*bagged.*pea|pea.*\d+.*bagged",10),
        ],
    },

    "electrical": {
        "junction_boxes_and_accessories/split_bolts_and_connectors": [
            (r"split bolt|flat head split bolt",10),
        ],
        "junction_boxes_and_accessories/access_panels": [
            (r"access panel|oatey.*access|oater.*access",10),
        ],
        "power_distribution/extension_cords_electrical": [
            (r"ext.*cord|extension.*cord|\bext cord\b",9),
        ],
        "power_distribution/gfci_cords_and_blocks": [
            (r"\bgfci\b|\bgfi\b|cep.*power block|cep.*triple tap",10),
        ],
        "power_distribution/power_strips_and_adapters": [
            (r"power strip|surge protector|3.*outlet.*adapter|3 way.*cord|go green",9),
        ],
        "wire_and_cable/electrical_tape": [
            (r"electrical tape|pvc.*electrical tape|satco.*electrical|tuff stuff.*electrical tape",9),
        ],
        "wire_and_cable/romex_and_thhn": [
            (r"romex|thhn|\b12/3\b.*gfi|\bnm.?b\b",10),
        ],
    },

    "flooring": {
        "flooring_trim_and_transitions/radiant_heat_flooring": [
            (r"strata heat|radiant.*floor.*heat|heating.*cable.*floor|laticrete.*strata heat",10),
        ],
        "flooring_trim_and_transitions/flooring_protection": [
            (r"carpet film|floor.*film|protection.*film|g.force.*carpet film",10),
        ],
        "flooring_trim_and_transitions/flooring_installation_tools": [
            (r"floor chisel|dasco.*floor|floor scraper|red devil.*floor|\box.*knee pad",9),
        ],
        "tile_flooring/tile_setting_for_floors": [
            (r"tile mastic|bostik.*mastic|strata heat|laticrete.*strata",9),
        ],
        "tile_flooring/granite_tile": [
            (r"granite.*tile|\dgranite.*\d+.*x.*\d+|charcoal.*granite|gray.*granite",10),
        ],
        "hardwood_flooring/walnut_and_mixed_hardwood_flooring": [
            (r"walnut.*floor|american walnut natural|hardwood floor",9),
        ],
        "hardwood_flooring/white_oak_flooring": [
            (r"white oak.*floor|white oak.*\d+/\d+|white oak.*rough",10),
        ],
        "hardwood_flooring/red_oak_flooring": [
            (r"red oak.*floor|floor.*red oak|red oak.*\d+.*bd ft|red oak.*board.*ft",10),
        ],
        "luxury_vinyl_plank/lvp_standard_collections": [
            (r"\blvp\b|luxury vinyl plank|vinyl plank.*floor|floor.*lvp",9),
        ],
        "luxury_vinyl_plank/lvp_kilimanjaro_series": [
            (r"kilimanjaro|addis ababa|cairo hickory|cape town oak|casablanca oak|marrakesh pecan|zanzibar oak",10),
        ],
    },

    "siding": {
        "siding_caulks_and_sealants/general_siding_caulk": [
            (r"red devil.*lifetime ultra|red devil.*230|lifetime ultra",10),
        ],
        "siding_caulks_and_sealants/log_home_caulk_for_siding": [
            (r"\bcon\b.*(brown tone|frontier gold|harvest wheat|weathered gray|grizzly brown|red tone)",10),
            (r"log.*caulk.*siding|chinking.*siding",9),
        ],
        "fiber_cement_siding/fiber_cement_blades": [
            (r"fiber cement.*blade|ivy.*7.*fiber cement|fiber cement.*cut",10),
        ],
        "fiber_cement_siding/fiber_cement_planks": [
            (r"fiber cement.*plank|hardie plank|fiber cement siding",10),
        ],
        "cedar_and_wood_siding/incense_cedar_siding": [
            (r"\bic\b.*\d+x\d+x\d+|incense cedar|\bic\b.*cedar",10),
        ],
        "cedar_and_wood_siding/kiln_dried_pine_siding": [
            (r"pine.*kiln dried|kiln dried.*pine|\bpine\b.*1x\d+x\d+",9),
        ],
        "cedar_and_wood_siding/s1s2e_cedar_siding": [
            (r"s1s2e.*cedar|s1s2e.*1x.*cedar",10),
        ],
        "natural_edge_and_rustic_siding/rough_cut_siding_boards": [
            (r"rough cut.*1x12|1.*x.*12.*rough cut|ne siding variable",9),
        ],
        "natural_edge_and_rustic_siding/hand_peeled_natural_edge_siding": [
            (r"ne sid hp|hand peeled.*siding|natural edge.*hp",10),
        ],
        "natural_edge_and_rustic_siding/bark_on_natural_edge_siding": [
            (r"ne sid bark on|bark on.*siding|natural edge.*bark",10),
        ],
    },

    "kitchen_and_bath": {
        "cabinetry/cabinet_trim_lumber": [
            (r"cabinet.*trim lumber|cabinet trim.*\d+'|trim lumber.*cabinet",10),
        ],
        "waterproofing_and_wet_area/kitchen_bath_sealants": [
            (r"ge silicone.*kitchen|ge silicone.*bath|kitchen.*bath.*silicone|ge silicone 1 kitchen",10),
        ],
        "waterproofing_and_wet_area/hydro_ban_membrane": [
            (r"hydro.?ban",10),
        ],
        "outdoor_grills_and_appliances/grill_door_drawer_combos": [
            (r"coyote.*door.*drawer|stainless steel.*double door|stainless steel.*single door|door.*drawer.*combo|tank drawer",10),
        ],
        "outdoor_grills_and_appliances/coyote_side_burners": [
            (r"coyote.*side burner|coyote.*asado|coyote.*double.*burner",10),
        ],
        "outdoor_grills_and_appliances/coyote_grills_drop_in": [
            (r"coyote.*grill.*drop.?in|coyote.*34.*drop|coyote.*grill",9),
        ],
        "outdoor_kitchen_cabinetry/olde_english_kitchen_kits": [
            (r"\boe wall kitchen\b|olde english.*kitchen|oew fa|oew.*kitchen",10),
        ],
        "outdoor_kitchen_cabinetry/maytrx_kitchen_systems": [
            (r"\bmaytrx\b|\bmaytrxren\b",10),
        ],
        "outdoor_kitchen_cabinetry/fa_veneer_cabinets": [
            (r"fa veneer.*cabinet|faveneer|fa veneer.*kitchen|fa veneer.*double door|fa veneer.*refrigerator",10),
        ],
    },

    "plumbing": {
        "plumbing_tools/tubing_levels": [(r"tubing level|clear tubing level|master plumber.*level",10)],
        "plumbing_tools/adjustable_wrenches": [
            (r"adjustable wrench|ivy.*adjustable|tuff stuff.*wrench",9),
        ],
        "plumbing_tools/pipe_wrenches": [
            (r"pipe wrench|\d+.*pipe wrench|aluminum pipe wrench",10),
        ],
        "valves_and_controls/angle_valves": [
            (r"angle key valve|angle.*valve|key.*valve",10),
        ],
        "pipe_and_fittings/flexible_drain_pipe": [
            (r"flex.?drain.*4.*solid|solid.*flexible.*drain|corrugated.*4.*pipe|flex.?drain.*coupler|flex-drain",9),
        ],
        "pipe_and_fittings/pvc_pipe_and_fittings": [
            (r"pvc pipe|3/4.*pvc.*pipe|pvc.*pipe.*400|4.*tee.*pvc|pvc.*tee|y.connector|y connector",9),
        ],
    },

    "hvac": {
        "ventilation/pergola_ventilation": [
            (r"bioclimatic.*pergola|fan.*pergola|pergola.*fan",10),
        ],
        "ventilation/masonry_weep_vents": [
            (r"weep.*vent|quadro.?vent|weeps rect|masonry weep",10),
        ],
        "ductwork_and_registers/duct_tape": [
            (r"duct tape|dewalt.*duct tape|ipg.*duct tape|scotch.*duct tape|shurtape.*duct",9),
        ],
        "ductwork_and_registers/flue_pipe": [
            (r"flu pipe|flue pipe|round.*flu|rectangle.*flu|\bflu\b",9),
        ],
        "ductwork_and_registers/flex_duct": [
            (r"flex duct|mylar.*duct|\d+.*mylar.*duct",10),
        ],
        "heating_systems/radiant_floor_heating": [
            (r"strata heat|radiant.*heat.*system|floor heating|laticrete.*strata",10),
        ],
    },

    "framing_materials": {
        "masonry_joint_reinforcement/dur_o_wall_truss": [
            (r"dur.?o.?wall|dur-o-wall|truss.*masonry|masonry.*truss.*wire",10),
        ],
        "i_joists_and_rim_board/rim_board": [(r"rim board|rim joist|\bblocking\b",10)],
        "i_joists_and_rim_board/i_joists": [(r"i.?joist|\bi.?joist\b",10)],
        "engineered_lumber/psl_columns": [(r"\bpsl\b|parallel strand lumber|psl.*column",10)],
        "engineered_lumber/lvl_beams": [
            (r"\blvl\b|laminated veneer lumber|lvl beam",10),
        ],
        "dimensional_framing_lumber/douglas_fir_framing": [
            (r"douglas fir|doug fir|df.*2x|df.*4x|df.*6x|df.*8x",8),
        ],
    },

    "sheathing": {
        "sheathing_fasteners_and_accessories/osb_adhesive_brushes": [
            (r"osb brush|osb.*4.*brush|osb.*6.*brush",10),
        ],
        "sheathing_fasteners_and_accessories/cap_nails_sheathing": [
            (r"scs.*cap nails|cap nail.*scs|\bcap nails?\b.*25lb|cap nail.*25 lb",10),
        ],
        "structural_plywood/marine_and_premium_plywood": [
            (r"marine grade|a.b.*plywood|birch plywood|fir.*mill.*cert|birch.*plywood|3/4.*a.b",10),
        ],
        "structural_plywood/cdx_plywood": [
            (r"cdx plywood|\bcdx\b|plywood.*cdx|cdx.*plywood|1/2.*plywood|3/8.*plywood|5/8.*plywood|1/4.*plywood",9),
        ],
        "osb_sheathing/osb_variable": [(r"osb variable|osb.*variable",10)],
        "osb_sheathing/osb_standard": [
            (r"7/16.*osb|osb.*4.*x.*8|oriented strand board|\bosb\b",9),
        ],
    },

    "doors": {
        "door_hardware/window_sill_trim": [
            (r"canyon.*sill|\bwt sill\b|window.*sill.*canyon",10),
        ],
        "door_hardware/silicone_window_door": [
            (r"ge silicone.*window|ge silicone.*door|ge silicone 1 white window|red devil.*silicone|dap alex plus|ge.*window.*door",9),
        ],
        "temporary_construction_barriers/zip_wall_systems": [
            (r"zip wall|zipdoor|zip.wall",10),
        ],
        "interior_doors/mailbox_doors": [(r"mailbox door|\bmailbox\b",10)],
        "exterior_doors/outdoor_kitchen_doors": [
            (r"coyote.*door|stainless steel.*door|stainless.*single door|stainless.*double door",10),
        ],
    },

    "windows": {
        "window_hardware/window_cleaning_tools": [
            (r"window squeegee|ettore.*squeegee",10),
        ],
        "window_hardware/window_sealants": [
            (r"ge silicone.*window|dap alex plus|red devil.*silicone|window.*door.*sealant|ge silicone 1 white window",9),
        ],
        "specialty_glass_products/glass_block": [
            (r"glass block|p\.c\..*glass block|glass block.*decora",10),
        ],
        "vinyl_windows/vinyl_retrofit_windows": [
            (r"vinyl.*retrofit.*window|vinyl.*window|59\.5.*47\.5",9),
        ],
    },

    "miscellaneous": {
        "general_site_supplies/shop_and_facility_supplies": [
            (r"toilet paper|paper towel|mop bucket|sweeping compound",10),
        ],
        "general_site_supplies/waste_disposal": [
            (r"contractor bag|garbage bag|trash bag|shop vac|wet.*dry.*vac",9),
        ],
        "general_site_supplies/buckets_and_containers": [
            (r"\bbucket\b|garbage can|trash can|gas can|fuel can|\bgallon.*bucket\b|recycling tub",8),
        ],
        "outdoor_structures/pergola_kits": [
            (r"pergola kit|\d+x\d+.*pergola|pergola.*\d+x\d+|fiberglass.*pergola|\bpergola\b",9),
        ],
        "outdoor_structures/pavilion_kits": [
            (r"pavilion kit|10.*15.*pavilion|12.*18.*pavilion|16.*20.*pavilion",9),
        ],
        "cleaning_and_maintenance/ice_melt_and_treatment": [
            (r"calcium chloride|ice melt|\bice.?melt\b",10),
        ],
        "cleaning_and_maintenance/lubricants_and_greases": [
            (r"\bwd.?40\b|grease.*bucket|kerosene|\blubricant\b|10lb.*grease",9),
        ],
        "cleaning_and_maintenance/solvents_and_cleaners": [
            (r"\bacetone\b|lacquer thinner|fast orange|\bbleach\b|\bsolvent\b",9),
        ],
        "delivery_and_service_fees/pallet_returns": [(r"pallet.*return|return.*pallet",10)],
        "delivery_and_service_fees/labor_charges": [
            (r"labor.*charge|1day labor|1 day labor|day labor",10),
        ],
        "delivery_and_service_fees/delivery_fees": [
            (r"delivery.*fee|delivery.*charge|shipping.*delivery",10),
        ],
        "safety_and_ppe/site_safety_equipment": [
            (r"traffic cone|safety cone|\bcone\b.*orange|orange.*cone|safety flag|red safety flag",9),
        ],
        "safety_and_ppe/high_visibility_clothing": [
            (r"hi vis|high visibility|hi-vis|safety vest|rainsuit|\bcoveralls?\b|pyramex|reflective.*vest|\bvest\b.*safety",9),
        ],
        "safety_and_ppe/fall_protection": [
            (r"\bharness\b|\blanyard\b|rope grab|fall arrest|shock.*absorb.*lanyard",10),
        ],
        "safety_and_ppe/respiratory_protection": [
            (r"\brespirator\b|\bmask\b.*respirator|3m.*8511|\bn95\b",10),
        ],
        "safety_and_ppe/hand_protection": [
            (r"\bgloves?\b|\bglove\b|leather glove|mechanics glove|latex glove|nitrile glove|rubber.*glove|gauntlet glove",9),
        ],
        "safety_and_ppe/eye_and_face_protection": [
            (r"safety glass|safety goggle|face shield|\bgoggles?\b|honeywell.*v.maxx.*goggle|v-maxx",9),
        ],
        "safety_and_ppe/head_protection": [
            (r"hard hat|\bhelmet\b|north.*hard hat",10),
        ],
    },
}


# ── Load taxonomy to build leaf index and defaults ───────────────────────────
def build_leaf_index(taxonomy_path):
    """
    Returns:
        leaf_meta: {tier3_path -> (tier1, tier2_slug, tier3_slug)}
        default_leaf: {tier1 -> tier3_path}  # first tier3 of first tier2
    """
    with open(taxonomy_path) as f:
        data = json.load(f)

    leaf_meta = {}
    default_leaf = {}

    for t1_slug, t1 in data.items():
        first_leaf = None
        for sub in t1.get("subcategories", []):
            t2_slug = sub["slug"]
            for leaf in sub.get("tier3", []):
                t3_slug = leaf["slug"]
                path = f"{t2_slug}/{t3_slug}"
                leaf_meta[path] = (t1_slug, t2_slug, t3_slug)
                if first_leaf is None:
                    first_leaf = path
        if first_leaf:
            default_leaf[t1_slug] = first_leaf

    return leaf_meta, default_leaf


# ── Compile rules ─────────────────────────────────────────────────────────────
def compile_tier1():
    return {
        cat: [(re.compile(p, re.IGNORECASE), w) for p, w in rules]
        for cat, rules in RULES.items()
    }


def compile_tier3():
    return {
        t1: {
            path: [(re.compile(p, re.IGNORECASE), w) for p, w in rules]
            for path, rules in paths.items()
        }
        for t1, paths in TIER3_RULES.items()
    }


# ── Scoring ────────────────────────────────────────────────────────────────────
def get_tier1_cats(text, old_category, compiled_t1):
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, rules in compiled_t1.items():
        for pattern, weight in rules:
            if pattern.search(text):
                scores[cat] += weight
    for hint_cat in OLD_CAT_HINTS.get(old_category, []):
        if hint_cat in scores:
            scores[hint_cat] += HINT_BOOST

    sorted_cats = sorted(scores.items(), key=lambda x: -x[1])
    best_cat, best_score = sorted_cats[0]
    if best_score <= 2:
        return ["miscellaneous"]

    result = [best_cat]
    for cat, score in sorted_cats[1:]:
        if len(result) >= MAX_LEAF_CATS:
            break
        raw = score - (HINT_BOOST if cat in OLD_CAT_HINTS.get(old_category, []) else 0)
        if raw >= SECONDARY_THRESHOLD:
            result.append(cat)
    return result


def get_tier3_path(text, t1, compiled_t3, default_leaf):
    paths = compiled_t3.get(t1, {})
    best_path, best_score = None, 0
    for path, rules in paths.items():
        score = sum(w for pat, w in rules if pat.search(text))
        if score > best_score:
            best_score, best_path = score, path
    return best_path if best_path else default_leaf.get(t1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main(
    filter_ids=None,
    input_file=None,
    output_file=None,
):
    in_path = input_file or INPUT_FILE
    out_path_str = output_file or OUTPUT_FILE

    leaf_meta, default_leaf = build_leaf_index(TAXONOMY)
    compiled_t1 = compile_tier1()
    compiled_t3 = compile_tier3()

    rows_out = []
    seen = set()   # (item_id, tier3_path) deduplicate

    with open(in_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            item_id     = row.get("id") or ""
            if filter_ids is not None and item_id not in filter_ids:
                continue
            title       = row.get("title") or ""
            description = row.get("description") or ""
            subtitle    = row.get("subtitle") or ""
            old_cat     = row.get("category") or ""
            store_name  = row.get("store_name") or ""

            text = f"{title} {subtitle} {description}"
            t1_cats = get_tier1_cats(text, old_cat, compiled_t1)

            for t1 in t1_cats:
                t3_path = get_tier3_path(text, t1, compiled_t3, default_leaf)
                if not t3_path:
                    continue
                key = (item_id, t3_path)
                if key in seen:
                    continue
                seen.add(key)

                meta = leaf_meta.get(t3_path)
                if not meta:
                    continue
                _, t2_slug, t3_slug = meta
                rows_out.append({
                    "item_id":       item_id,
                    "item_name":     title,
                    "item_description": description,
                    "store_name":    store_name,
                    "tier1":         t1,
                    "tier2":         t2_slug,
                    "tier3":         t3_slug,
                    "category_path": f"{t1}/{t3_path}",
                })

    out_path = Path(out_path_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["item_id","item_name","item_description","store_name","tier1","tier2","tier3","category_path"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows_out)

    # ── Summary ────────────────────────────────────────────────────────────────
    from collections import Counter
    total_items = len(set(r["item_id"] for r in rows_out))
    total_rows  = len(rows_out)
    leaf_counts = Counter(r["category_path"] for r in rows_out)
    empty_leaves = set(leaf_meta.keys()) - {"/".join(p.split("/")[1:]) for p in leaf_counts}

    print(f"Total unique items:  {total_items:,}")
    print(f"Total mapping rows:  {total_rows:,}")
    print(f"Unique leaves hit:   {len(leaf_counts):,} / {len(leaf_meta):,}")
    print(f"Empty leaves:        {len(empty_leaves):,}")
    print(f"\nTop 30 leaf categories by row count:")
    for path, cnt in leaf_counts.most_common(30):
        print(f"  {cnt:5,}  {path}")
    print(f"\nEmpty leaves:")
    for lp in sorted(empty_leaves):
        print(f"  {lp}")
    print(f"\nOutput: {out_path}")


def main_cli():
    ap = argparse.ArgumentParser(description="Map items to leaf categories (step 0)")
    ap.add_argument("--input", default=INPUT_FILE, help="Input CSV path")
    ap.add_argument("--output", default=OUTPUT_FILE, help="Output CSV path")
    ap.add_argument(
        "--filter-file",
        help="Path to text file with one item_id per line; only those items are processed",
    )
    args = ap.parse_args()

    filter_ids: set[str] | None = None
    if args.filter_file:
        with open(args.filter_file, encoding="utf-8") as f:
            filter_ids = {line.strip() for line in f if line.strip()}
        print(f"Filtering to {len(filter_ids)} item IDs from {args.filter_file}")

    main(input_file=args.input, output_file=args.output, filter_ids=filter_ids)


if __name__ == "__main__":
    main_cli()
