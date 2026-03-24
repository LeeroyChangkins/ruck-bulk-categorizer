// Source: server/seed/fixtures/marketplace-categories.ts

export interface CategoryNode {
  id: string;
  name: string;
  slug: string;
  children?: CategoryNode[];
}

export const MARKETPLACE_ROOT_CATEGORY: CategoryNode = {
  id: "mkc_building_materials",
  name: "Building Materials",
  slug: "building-materials",
  children: [
    { id: "mkc_concrete_cement_and_masonry", name: "Concrete, Cement & Masonry", slug: "concrete-cement-masonry" },
    { id: "mkc_construction_adhesives_and_sealants", name: "Construction Adhesives & Sealants", slug: "construction-adhesives-sealants" },
    { id: "mkc_decking", name: "Decking", slug: "decking" },
    { id: "mkc_dimensional_lumber_and_composites", name: "Dimensional Lumber & Composites", slug: "dimensional-lumber-composites" },
    { id: "mkc_doors", name: "Doors", slug: "doors" },
    { id: "mkc_drywall_and_accessories", name: "Drywall & Accessories", slug: "drywall-accessories" },
    { id: "mkc_electrical", name: "Electrical", slug: "electrical" },
    { id: "mkc_fencing", name: "Fencing", slug: "fencing" },
    { id: "mkc_flooring", name: "Flooring", slug: "flooring" },
    { id: "mkc_framing_materials", name: "Framing Materials", slug: "framing-materials" },
    { id: "mkc_general_fasteners_and_hardware", name: "General Fasteners & Hardware", slug: "general-fasteners-hardware" },
    { id: "mkc_hvac", name: "HVAC", slug: "hvac" },
    { id: "mkc_insulation", name: "Insulation", slug: "insulation" },
    { id: "mkc_kitchen_and_bath", name: "Kitchen & Bath", slug: "kitchen-bath" },
    { id: "mkc_metals_and_metal_fabrication", name: "Metals & Metal Fabrication", slug: "metals-metal-fabrication" },
    { id: "mkc_miscellaneous", name: "Miscellaneous", slug: "miscellaneous" },
    { id: "mkc_paint_and_stain", name: "Paint & Stain", slug: "paint-stain" },
    { id: "mkc_plants_and_landscaping", name: "Plants & Landscaping", slug: "plants-landscaping" },
    { id: "mkc_plumbing", name: "Plumbing", slug: "plumbing" },
    { id: "mkc_rebar_and_reinforcement", name: "Rebar & Reinforcement", slug: "rebar-reinforcement" },
    { id: "mkc_roofing_materials", name: "Roofing Materials", slug: "roofing-materials" },
    { id: "mkc_sheathing", name: "Sheathing", slug: "sheathing" },
    { id: "mkc_siding", name: "Siding", slug: "siding" },
    { id: "mkc_sitework_and_drainage", name: "Sitework & Drainage", slug: "sitework-drainage" },
    { id: "mkc_structural_fasteners_and_connectors", name: "Structural Fasteners & Connectors", slug: "structural-fasteners-connectors" },
    { id: "mkc_timber_logs_and_specialty_wood", name: "Timber, Logs & Specialty Wood", slug: "timber-logs-specialty-wood" },
    { id: "mkc_tools", name: "Tools", slug: "tools" },
    { id: "mkc_weatherproofing_house_wrap", name: "Weatherproofing & House Wrap", slug: "weatherproofing-house-wrap" },
    { id: "mkc_windows", name: "Windows", slug: "windows" },
  ],
};
