#!/usr/bin/env python3
"""
Produce a flat item-category mapping table.

Output: items-category-mapping.csv
  Columns: item_id, item_name, item_description, store_name, category

One row per item-category pair. An item may appear in up to 3 rows if
multiple categories score >= SECONDARY_THRESHOLD independently.
The primary category (highest score) is always included.
"""

import csv
import re

INPUT_FILE = "items-with-store-data-2.csv"
OUTPUT_FILE = "items-category-mapping.csv"

# A secondary category is only included if it reaches this score on its own
SECONDARY_THRESHOLD = 9
MAX_CATEGORIES_PER_ITEM = 3

CATEGORIES = [
    "concrete_cement_and_masonry",
    "construction_adhesives_and_sealants",
    "decking",
    "dimensional_lumber_and_composites",
    "doors",
    "drywall_and_accessories",
    "electrical",
    "fencing",
    "flooring",
    "framing_materials",
    "general_fasteners_and_hardware",
    "hvac",
    "insulation",
    "kitchen_and_bath",
    "metals_and_metal_fabrication",
    "miscellaneous",
    "paint_and_stain",
    "plants_and_landscaping",
    "plumbing",
    "rebar_and_reinforcement",
    "roofing_materials",
    "sheathing",
    "siding",
    "sitework_and_drainage",
    "structural_fasteners_and_connectors",
    "timber_logs_and_specialty_wood",
    "tools",
    "weatherproofing_house_wrap",
    "windows",
]

RULES = {
    "fencing": [
        (r"barbed wire", 10),
        (r"field fence", 10),
        (r"cattle panel", 10),
        (r"hog panel", 10),
        (r"horse fence", 10),
        (r"non.?climb", 10),
        (r"corral panel", 10),
        (r"mesh gate", 10),
        (r"rail gate", 10),
        (r"square corner.*gate|round corner.*gate|5.rail.*gate", 10),
        (r"\bt-post\b|t post driver", 10),
        (r"fence stay", 10),
        (r"game fence", 10),
        (r"deer.*fence|deer gate|rabbit.*fence", 10),
        (r"poultry.*nett", 10),
        (r"split rail", 9),
        (r"vinyl fenc", 10),
        (r"welded wire", 7),
        (r"post pounder|post driver", 7),
        (r"\bfenc(e|ing)\b", 5),
        (r"\bline post\b|\bcorner post\b|\bblank post\b", 8),
    ],
    "timber_logs_and_specialty_wood": [
        (r"hand.?peeled", 10),
        (r"\blogs?\b", 7),
        (r"\bslabs?\b", 8),
        (r"live edge", 9),
        (r"natural edge slab", 10),
        (r"glulam", 9),
        (r"reclaimed timber", 10),
        (r"post.{0,5}beam", 8),
        (r"\bmantel\b|\bmantle\b", 9),
        (r"lodge.?pole", 10),
        (r"hand.?hewn", 10),
        (r"teepee pole", 10),
        (r"\bfirewood\b", 9),
        (r"walnut.*slab|black walnut", 9),
        (r"maple.*cookie", 10),
        (r"milled.?&.?peeled|milled and peeled", 9),
        (r"\bmilled\b", 7),
        (r"\btimbers?\b", 7),
        (r"natural edge", 8),
        (r"heart.?pine", 9),
        (r"\braw\b", 5),
        (r"rough.?cut.*lumber|rough.?sawn", 7),
        (r"\bbeam\b", 5),
        (r"wood.*slab|timber.*slab", 9),
        (r"\bpoplar\b|\bhickory\b|\bwalnut\b|\belm\b|\bash\b|\bcatalpa\b|\bsycamore\b|\bcottonwood\b|\blocust\b|\bcypress\b|\bcherry\b|\bmahogany\b", 7),
        (r"sawdust|wood.*chip", 6),
        (r"\bslab\b", 8),
        (r"\bwood board\b|\bhardwood board\b|\boak board\b|\bmaple board\b|\bash board\b|\bfull.dimension board\b", 9),
        (r"\b\dx\d+x\d+\b", 7),
        (r"carob round|wood round", 8),
    ],
    "dimensional_lumber_and_composites": [
        (r"\b[12]x[2-9]\d*\b", 9),
        (r"df #?1|df fohc", 9),
        (r"douglas.?fir", 8),
        (r"\bspruce\b", 7),
        (r"kiln.?dried", 8),
        (r"\bs4s\b", 9),
        (r"\bs1s2e\b", 9),
        (r"\bs2s\b", 9),
        (r"pressure.?treated", 8),
        (r"treated.*lumber|treated.*board", 8),
        (r"dimensional.?lumber", 10),
        (r"\bstud\b", 7),
        (r"df.*select|df select", 9),
        (r"\bprecut\b", 8),
        (r"\blumber\b", 4),
    ],
    "decking": [
        (r"composite.?deck", 10),
        (r"pvc.?deck", 10),
        (r"deck.?board", 10),
        (r"grooved.?board", 9),
        (r"starter.?board", 9),
        (r"picture.?frame.*board", 9),
        (r"fascia.*board", 8),
        (r"stair.?riser", 9),
        (r"\bdeck(ing)?\b", 7),
        (r"fortress rail", 9),
        (r"timbertech.*rail|\btimbertech\b", 9),
        (r"royal guard rail", 9),
        (r"ogden deck", 9),
        (r"cinch rail", 9),
        (r"\brail kit\b", 7),
        (r"\bbalusters?\b", 8),
        (r"\bacre.*trim\b", 8),
        (r"\bada.*joint\b|\bada.*lineal\b", 8),
        (r"privacy screen system", 8),
        (r"\bdck\b", 9),
        (r"\bdeckorators\b", 9),
        (r"composite.*legacy|pvc.*vintage|pvc.*reserve|composite.*collection", 9),
    ],
    "siding": [
        (r"natural edge siding", 10),
        (r"\bsiding\b", 9),
        (r"lap siding", 10),
        (r"board.*batten", 9),
        (r"\bt1-11\b", 10),
        (r"hardie", 9),
        (r"fiber.?cement.*sid", 10),
        (r"vinyl siding", 10),
        (r"cedar siding", 10),
        (r"\bshiplap\b", 9),
    ],
    "roofing_materials": [
        (r"\broofing\b", 9),
        (r"\bshingle\b", 10),
        (r"\bunderlayment\b", 9),
        (r"ridge.?cap", 10),
        (r"drip.?edge", 10),
        (r"roof.*felt", 9),
        (r"ice.*water.*shield", 9),
        (r"roofing.*nail", 9),
        (r"\bgutter\b", 8),
        (r"roofing.*trim", 9),
        (r"roofing.*product", 8),
        (r"roofing.*flashing", 9),
        (r"roof.*coating|silicone.*roof", 9),
        (r"termination.*bar", 8),
    ],
    "sheathing": [
        (r"\bosb\b", 9),
        (r"oriented.?strand.?board", 10),
        (r"\bplywood\b", 9),
        (r"\bcdx\b", 10),
        (r"structural.*panel", 9),
        (r"\bsheathing\b", 10),
    ],
    "framing_materials": [
        (r"\bjoist\b", 9),
        (r"\brafter\b", 9),
        (r"\bheader\b", 8),
        (r"\btruss\b", 9),
        (r"i.?joist", 10),
        (r"\blvl\b", 9),
        (r"laminated.*veneer", 9),
        (r"framing.*lumber|framing.*material", 10),
        (r"rim.*joist", 9),
        (r"glulam.*beam|glulam.*frame", 8),
    ],
    "concrete_cement_and_masonry": [
        (r"concrete.*mix", 10),
        (r"\bcement\b", 9),
        (r"\bmortar\b", 9),
        (r"\bgrout\b", 8),
        (r"\bbricks?\b", 8),
        (r"concrete.*block|cinder.*block", 9),
        (r"\bpavers?\b", 9),
        (r"\bstucco\b", 9),
        (r"\bplaster\b", 8),
        (r"thin.?set|thinset", 9),
        (r"\blaticrete\b", 9),
        (r"thin.*brick", 9),
        (r"concrete.*color", 9),
        (r"masonry.*coat|paint.*masonry|masonry.*paint", 8),
        (r"\bconcrete\b", 7),
        (r"cement.*pigment|\bpigment\b", 8),
        (r"\bmasonry\b", 7),
        (r"\bbluestone\b", 8),
        (r"\bflagstone\b", 8),
        (r"\bgranite\b", 7),
        (r"lehigh.*color|lehigh.*cement", 9),
        (r"joint.*sand|polymeric.*sand", 8),
        (r"ledgestone|ledger.*stone", 8),
        (r"setting.*material", 7),
        (r"concrete.*wire.*lath|wire.*lath", 8),
        (r"\bcoping\b", 8),
        (r"retaining.*wall|wall.*block|segmental.*wall", 9),
        (r"split.*face.*wall|split.*face.*corner|split.*face.*stretcher", 9),
        (r"\bpier cap\b|\bcolumn kit\b", 8),
        (r"\bfirepit\b|\bfire pit\b", 7),
        (r"\btravertine\b|\bslatestone\b|\bcobblestone\b|\bcobble\b", 8),
        (r"\bedger\b|\bedging\b", 6),
        (r"\bcapstone\b", 9),
        (r"\bnicostone\b|\bbradstone\b|\bbelvedere\b|\bmadoc\b|\bkodah\b|\bverona\b", 8),
        (r"\bfullnose\b", 8),
        (r"\bscarf\b", 6),
        (r"limestone.*tread|limestone.*cap|limestone.*wall|limestone.*block|limestone.*rockface", 9),
        (r"\bgeogrid\b|\bgator.*grid\b", 8),
        (r"natural.*stone.*cap|stone.*flat.*cap", 8),
        (r"mini colonial|olde english.*wall|cambridge.*wall|sigma.*wall|maytrx.*wall|omega.*wall|stonehenge", 9),
        (r"\bblacktop\b|blacktop.*patch", 9),
        (r"quikrete", 8),
        (r"\bzamac\b", 8),
        (r"super.*sand|alliance.*sand", 8),
    ],
    "rebar_and_reinforcement": [
        (r"\brebar\b", 10),
        (r"steel.*rebar", 10),
        (r"\bre-rod\b", 10),
        (r"deformed.*bar", 10),
        (r"reinforcing.*bar", 10),
        (r"concrete.*lath", 8),
        (r"tie.*wire", 8),
        (r"reinforc(e|ement|ing)", 7),
    ],
    "metals_and_metal_fabrication": [
        (r"square.*tube", 9),
        (r"rectangular.*tube|rectangle.*tube", 9),
        (r"carbon.*steel", 10),
        (r"steel.*channel|channel.*a36", 9),
        (r"angle.*iron", 9),
        (r"flat.*bar", 8),
        (r"steel.*plate|steel.*sheet|1095.*steel.*sheet", 9),
        (r"\ba36\b|\ba500\b", 9),
        (r"\bsteel\b", 4),
        (r"\bmetal\b", 4),
    ],
    "general_fasteners_and_hardware": [
        (r"\bscrews?\b", 7),
        (r"\bnails?\b", 7),
        (r"\bbolts?\b", 7),
        (r"\bwashers?\b", 7),
        (r"\bfastener\b", 8),
        (r"\bstaple\b", 7),
        (r"\bhinges?\b", 9),
        (r"\bbrackets?\b", 7),
        (r"nails.*screws|screws.*pins|hardware.*bits|bits.*batteries", 9),
        (r"deck.*screw", 8),
    ],
    "structural_fasteners_and_connectors": [
        (r"joist.*hanger", 10),
        (r"post.*base|post.*cap", 9),
        (r"beam.*connector", 10),
        (r"hurricane.*tie", 10),
        (r"framing.*connector", 10),
        (r"structural.*screw", 9),
        (r"lag.*bolt", 9),
        (r"\bsimpson\b", 9),
        (r"tension.*tie", 9),
        (r"hold.?down", 9),
    ],
    "construction_adhesives_and_sealants": [
        (r"\bcaulks?\b", 9),
        (r"\bsealants?\b", 9),
        (r"\badhesive\b", 9),
        (r"backer.*rod", 9),
        (r"grip.*strip", 9),
        (r"\bnp.*1\b|\bsonneborn\b", 9),
        (r"sashco.*caulk|sashco.*sealant", 10),
        (r"wood.*epoxy", 9),
        (r"construction.*adhesive", 10),
        (r"sashco.*backer|sashco.*preserve|sashco.*prep", 10),
        (r"expanding.*foam|touch.*n.*foam|great.*stuff.*foam", 9),
        (r"\bepoxy.*anchor\b|\bhilti.*epoxy\b|\bepoxy.*sleeve\b", 8),
    ],
    "paint_and_stain": [
        (r"sashco.*stain", 10),
        (r"\bstains?\b", 8),
        (r"\bpaints?\b", 8),
        (r"\bprimer\b", 9),
        (r"wood.*stain|exterior.*stain", 9),
        (r"masonry.*coating|paint.*coating", 9),
        (r"\bpigment\b", 7),
        (r"preserv(e|ative|er)", 7),
        (r"sashco.*preserve|sashco.*prep", 9),
        (r"paint.*masonry|masonry.*paint", 9),
        (r"\broller\b.*refill|\broller.*sleeve\b|\broller.*frame\b|\bnap.*roller\b|roller.*cover", 7),
    ],
    "insulation": [
        (r"\binsulation\b", 10),
        (r"\bbatt\b", 9),
        (r"blown.?in", 9),
        (r"spray.*foam", 9),
        (r"rigid.*foam", 9),
        (r"r-value", 9),
        (r"vapor.*barrier", 8),
        (r"weather.*barrier", 8),
        (r"insul.*roll|insul.*board", 9),
        (r"pipe.*insulation", 9),
    ],
    "weatherproofing_house_wrap": [
        (r"house.*wrap", 10),
        (r"\btyvek\b", 10),
        (r"building.*wrap", 10),
        (r"flashing.*tape", 10),
        (r"butyl.*tape", 9),
        (r"\bweatherproof", 9),
        (r"poly.*sheeting|poly.*sheet", 8),
        (r"\btarps?\b", 7),
        (r"shrink.*wrap", 8),
    ],
    "windows": [
        (r"\bwindows?\b", 10),
        (r"\bcasement\b", 9),
        (r"double.?hung", 9),
        (r"sliding.*window", 9),
        (r"awning.*window", 9),
        (r"\bskylight\b", 9),
        (r"vinyl.*window", 10),
    ],
    "doors": [
        (r"\bdoors?\b", 9),
        (r"barn.*door", 10),
        (r"sliding.*door", 9),
        (r"entry.*door|exterior.*door|interior.*door", 10),
    ],
    "flooring": [
        (r"\blvp\b", 10),
        (r"\bspc\b", 9),
        (r"lvp.*floor|spc.*floor", 10),
        (r"luxury.*vinyl.*plank|vinyl.*plank", 9),
        (r"hardwood.*floor|laminate.*floor", 9),
        (r"\bflooring\b", 8),
        (r"floor.*tile", 8),
        (r"premier.*collection|galaxy.*collection|express.*collection", 8),
    ],
    "kitchen_and_bath": [
        (r"\bkitchen\b", 8),
        (r"\bbath\b", 7),
        (r"\bcabinet\b", 9),
        (r"\bcountertop\b", 9),
        (r"\bsink\b", 8),
        (r"\bfaucet\b", 9),
        (r"\btoilet\b", 9),
        (r"\bshower\b", 8),
        (r"\bvanity\b", 9),
    ],
    "plumbing": [
        (r"\bplumbing\b", 10),
        (r"pvc.*pipe|copper.*pipe", 9),
        (r"water.*heater", 9),
        (r"\bvalves?\b", 7),
        (r"drain.*pipe|drainage.*pipe", 8),
    ],
    "electrical": [
        (r"\belectrical\b", 10),
        (r"\bconduit\b", 9),
        (r"\boutlets?\b", 9),
        (r"\bbreakers?\b", 9),
        (r"\bromex\b", 9),
        (r"junction.*box", 9),
        (r"electrical.*wire", 9),
    ],
    "hvac": [
        (r"\bhvac\b", 10),
        (r"\bducts?\b", 9),
        (r"\bfurnace\b", 9),
        (r"air.*handler", 9),
        (r"heating.*system", 8),
        (r"\bventilation\b", 8),
    ],
    "drywall_and_accessories": [
        (r"\bdrywall\b", 10),
        (r"\bsheetrock\b", 10),
        (r"\bgypsum\b", 9),
        (r"joint.*compound", 10),
        (r"corner.*bead", 9),
        (r"drywall.*screw|drywall.*tape|drywall.*mud", 9),
    ],
    "sitework_and_drainage": [
        (r"drainage.*pipe|drain.*pipe.*cover|pipe.*cover", 9),
        (r"\bculvert\b", 9),
        (r"\bgeotextile\b", 9),
        (r"erosion.*control", 9),
        (r"french.*drain", 9),
        (r"catch.*basin", 9),
        (r"silt.*fence", 9),
        (r"drainage pipe", 9),
    ],
    "plants_and_landscaping": [
        (r"\bmulch\b", 9),
        (r"\bsoil\b", 7),
        (r"\blandscap", 8),
        (r"\bgarden\b", 7),
        (r"\bgravel\b", 7),
        (r"sand.*gravel|gravel.*sand", 8),
        (r"\blimestone\b", 7),
        (r"wood.*chips?\b", 9),
        (r"\bplants?\b", 8),
        (r"\bshrubs?\b", 9),
        (r"\bsod\b", 9),
        (r"\btopsoil\b", 9),
        (r"mesquite|acacia|eucalyptus|pecan|mulberry|rosewood|sisso|citrus.*wood", 8),
        (r"\bsand\b", 5),
        (r"firewood.*bundle|bundle.*firewood|cubic.*firewood|pallet.*firewood|suv.*load.*firewood|truckload.*firewood", 9),
    ],
    "tools": [
        (r"\bdrill\b", 9),
        (r"\bsaw\b", 8),
        (r"\bhammers?\b", 9),
        (r"\bwrenches?\b", 9),
        (r"utility.*knife", 9),
        (r"\bchisel\b", 9),
        (r"\brouter\b", 8),
        (r"\bgrinder\b", 9),
        (r"\bsander\b", 9),
        (r"power.*tool|hand.*tool", 9),
        (r"\bstapler\b", 8),
        (r"\bnailer\b", 9),
        (r"mixing.*paddle", 9),
        (r"sawzall|diamond.*cut.*off|diamond.*saw", 9),
        (r"forstner.*bit", 10),
        (r"tenon.*cutter", 10),
        (r"draw.*knife", 10),
        (r"\blumberjack\b", 9),
        (r"caulk.*gun", 8),
        (r"machine.*rental", 7),
        (r"masonry.*accessor", 8),
        (r"\btools?\b", 4),
        (r"\btrowel\b|\bfresno\b|\bcome.along\b", 8),
        (r"mason.*line|chalk.*reel|\bchalk\b", 7),
        (r"diamond.*blade|saw.*blade|\bblade\b", 8),
        (r"\blevel\b", 7),
        (r"\bscraper\b|\bfloat\b|\bscreed\b", 7),
        (r"\bmarshalltown\b|\bstringliner\b|\birwin\b", 8),
        (r"\bwd-40\b", 6),
        (r"\bstihl\b", 9),
        (r"\broller\b.*refill|\broller.*sleeve\b|\broller.*frame\b|\bnap.*roller\b|roller.*cover", 7),
        (r"\bbroom\b|\bshovel\b|\brake\b|\bpick\b", 7),
        (r"\bcup.*wheel\b|\bdiamond.*cup\b|\bdiamond.*grind", 8),
        (r"\bsds.*plus\b|\bsds.*bit\b|\bsds.*max\b", 8),
        (r"\bbit.*holder\b|\bnut.*setter\b|\bhex.*setter\b", 7),
        (r"\bchain.*loop\b|\bchainsaw\b|\bblower\b", 8),
        (r"\bcaulk.*gun\b|\bgrease.*gun\b|\bhose.*nozzle\b", 7),
        (r"\bextension.*cord\b", 6),
        (r"\btape.*measure\b", 7),
        (r"\bscrewdriver\b", 7),
        (r"\bsocket\b|\bwrench\b", 7),
        (r"\brope\b|\bcord\b", 4),
    ],
    "miscellaneous": [
        (r"cut.*fee|cut_fee", 7),
        (r"delivery.*charge|\bdelivery fee\b", 7),
        (r"shipping.*delivery|delivery.*shipping", 7),
        (r"safety.*product", 8),
        (r"rental.*income", 7),
        (r"state.*tax", 7),
        (r"misc.*fee|\bmisc\b", 5),
    ],
}

OLD_CAT_HINTS = {
    "lumber_and_composites": ["timber_logs_and_specialty_wood", "dimensional_lumber_and_composites", "decking", "fencing", "siding"],
    "building_materials": ["concrete_cement_and_masonry", "metals_and_metal_fabrication", "rebar_and_reinforcement", "roofing_materials"],
    "hardware": ["general_fasteners_and_hardware", "tools", "structural_fasteners_and_connectors"],
    "tools": ["tools", "general_fasteners_and_hardware", "decking", "fencing"],
    "paint_supplies": ["paint_and_stain", "construction_adhesives_and_sealants"],
    "outdoors": ["plants_and_landscaping", "fencing", "sitework_and_drainage", "concrete_cement_and_masonry"],
    "safety": ["miscellaneous"],
    "cleaning_janitorial": ["miscellaneous", "construction_adhesives_and_sealants"],
    "flooring": ["flooring"],
    "bath": ["kitchen_and_bath"],
    "kitchen": ["kitchen_and_bath"],
    "hvac": ["hvac", "insulation"],
    "plumbing": ["plumbing"],
    "electrical": ["electrical"],
    "decor_and_furniture": ["timber_logs_and_specialty_wood", "miscellaneous"],
    "doors_and_windows": ["doors", "windows"],
    "storage_organization": ["miscellaneous"],
}

HINT_BOOST = 2


def compile_rules():
    return {
        cat: [(re.compile(p, re.IGNORECASE), w) for p, w in rules]
        for cat, rules in RULES.items()
    }


def get_categories_for_item(text, old_category, compiled_rules):
    scores = {cat: 0 for cat in CATEGORIES}

    for cat, rules in compiled_rules.items():
        for pattern, weight in rules:
            if pattern.search(text):
                scores[cat] += weight

    for hint_cat in OLD_CAT_HINTS.get(old_category, []):
        if hint_cat in scores:
            scores[hint_cat] += HINT_BOOST

    sorted_cats = sorted(scores.items(), key=lambda x: -x[1])
    best_cat, best_score = sorted_cats[0]

    # Always include the primary category (even if score is low, fall back to miscellaneous)
    if best_score <= 2:
        return ["miscellaneous"]

    result = [best_cat]

    # Add secondary/tertiary categories that independently meet the threshold
    for cat, score in sorted_cats[1:]:
        if len(result) >= MAX_CATEGORIES_PER_ITEM:
            break
        # Must score >= threshold on its own (before hint boost) to qualify as a real secondary
        raw_score = score - (HINT_BOOST if cat in OLD_CAT_HINTS.get(old_category, []) else 0)
        if raw_score >= SECONDARY_THRESHOLD:
            result.append(cat)

    return result


def main():
    compiled_rules = compile_rules()
    rows_out = []

    with open(INPUT_FILE, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            item_id = row.get("id") or ""
            title = row.get("title") or ""
            description = row.get("description") or ""
            subtitle = row.get("subtitle") or ""
            old_category = row.get("category") or ""
            store_name = row.get("store_name") or ""

            combined_text = f"{title} {subtitle} {description}"
            categories = get_categories_for_item(combined_text, old_category, compiled_rules)

            for cat in categories:
                rows_out.append({
                    "item_id": item_id,
                    "item_name": title,
                    "item_description": description,
                    "store_name": store_name,
                    "category": cat,
                })

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["item_id", "item_name", "item_description", "store_name", "category"])
        writer.writeheader()
        writer.writerows(rows_out)

    # Summary
    total_items = len(set(r["item_id"] for r in rows_out))
    total_rows = len(rows_out)
    multi_count = total_rows - total_items

    from collections import Counter
    cat_counts = Counter(r["category"] for r in rows_out)
    items_per_cat_count = Counter(
        len([r for r in rows_out if r["item_id"] == item_id])
        for item_id in set(r["item_id"] for r in rows_out)
    )

    print(f"Total unique items: {total_items:,}")
    print(f"Total mapping rows: {total_rows:,}")
    print(f"Extra rows from multi-category: {multi_count:,}")
    print(f"\nItems by category count:")
    for n in sorted(items_per_cat_count):
        print(f"  {n} category/categories: {items_per_cat_count[n]:,} items")
    print(f"\nCategory breakdown:")
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count:,}")
    print(f"\nOutput written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
