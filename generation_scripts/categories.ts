// Source: packages/core/src/constants/items.ts

export type Category =
  | "appliances"
  | "bath"
  | "blinds_and_window_treatments"
  | "building_materials"
  | "cleaning_janitorial"
  | "concrete_cement_and_masonry"
  | "construction_adhesives_and_sealants"
  | "decking"
  | "decor_and_furniture"
  | "dimensional_lumber_and_composites"
  | "doors"
  | "doors_and_windows"
  | "drywall_and_accessories"
  | "electrical"
  | "fencing"
  | "flooring"
  | "framing_materials"
  | "general_fasteners_and_hardware"
  | "hardware"
  | "holiday_decorations"
  | "hvac"
  | "insulation"
  | "kitchen"
  | "kitchen_and_bath"
  | "lighting"
  | "lumber_and_composites"
  | "metals_and_metal_fabrication"
  | "miscellaneous"
  | "outdoors"
  | "paint_and_stain"
  | "paint_supplies"
  | "plants_and_landscaping"
  | "plumbing"
  | "rebar_and_reinforcement"
  | "roofing_materials"
  | "safety"
  | "sheathing"
  | "siding"
  | "sitework_and_drainage"
  | "smart_home"
  | "storage_organization"
  | "structural_fasteners_and_connectors"
  | "timber_logs_and_specialty_wood"
  | "tools"
  | "weatherproofing_house_wrap"
  | "windows";

/** All 43 DB-valid categories (includes legacy). Used for reading/display. */
export const READ_CATEGORIES: Category[] = [
  // Legacy (read-only, pending migration)
  "appliances",
  "bath",
  "blinds_and_window_treatments",
  "building_materials",
  "cleaning_janitorial",
  "decor_and_furniture",
  "doors_and_windows",
  "hardware",
  "holiday_decorations",
  "kitchen",
  "lighting",
  "lumber_and_composites",
  "outdoors",
  "paint_supplies",
  "safety",
  "smart_home",
  "storage_organization",
  // Current
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
];

/** 29 current categories used when creating new items. */
export const WRITE_CATEGORIES: Category[] = [
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
];

export type WriteCategory = (typeof WRITE_CATEGORIES)[number];

export const CATEGORY_DISPLAY_NAMES: Record<Category, string> = {
  // Legacy
  appliances: "Appliances",
  bath: "Bath",
  blinds_and_window_treatments: "Blinds & Window Treatments",
  building_materials: "Building Materials",
  cleaning_janitorial: "Cleaning & Janitorial",
  decor_and_furniture: "Decor & Furniture",
  doors_and_windows: "Doors & Windows",
  hardware: "Hardware",
  holiday_decorations: "Holiday Decorations",
  kitchen: "Kitchen",
  lighting: "Lighting",
  lumber_and_composites: "Lumber & Composites",
  outdoors: "Outdoors",
  paint_supplies: "Paint Supplies",
  safety: "Safety",
  smart_home: "Smart Home",
  storage_organization: "Storage & Organization",
  // Current
  concrete_cement_and_masonry: "Concrete, Cement & Masonry",
  construction_adhesives_and_sealants: "Construction Adhesives & Sealants",
  decking: "Decking",
  dimensional_lumber_and_composites: "Dimensional Lumber & Composites",
  doors: "Doors",
  drywall_and_accessories: "Drywall & Accessories",
  electrical: "Electrical",
  fencing: "Fencing",
  flooring: "Flooring",
  framing_materials: "Framing Materials",
  general_fasteners_and_hardware: "General Fasteners & Hardware",
  hvac: "Heating, Venting & Cooling (HVAC)",
  insulation: "Insulation",
  kitchen_and_bath: "Kitchen & Bath",
  metals_and_metal_fabrication: "Metals & Metal Fabrication",
  miscellaneous: "Miscellaneous",
  paint_and_stain: "Paint & Stain",
  plants_and_landscaping: "Plants & Landscaping",
  plumbing: "Plumbing",
  rebar_and_reinforcement: "Rebar & Reinforcement",
  roofing_materials: "Roofing Materials",
  sheathing: "Sheathing",
  siding: "Siding",
  sitework_and_drainage: "Sitework & Drainage",
  structural_fasteners_and_connectors: "Structural Fasteners & Connectors",
  timber_logs_and_specialty_wood: "Timber, Logs & Specialty Wood",
  tools: "Tools",
  weatherproofing_house_wrap: "Weatherproofing / House Wrap",
  windows: "Windows",
};
