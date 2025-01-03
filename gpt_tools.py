#%%
from pydantic import BaseModel
from openai import OpenAI
# import google.generativeai as genai
import json

import base64
import os

# load from .env
from dotenv import load_dotenv
load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
gemini_api_key = os.getenv("GEMINI_API_KEY")
openai_client = OpenAI(api_key=openai_api_key)
# gemini_client = genai.configure(api_key=gemini_api_key)
# gemini_model = genai.GenerativeModel("gemini-1.5-flash")

from enum import Enum
from typing import Annotated

class ConstructionPhaseEnum(str, Enum):
    groundworks = "GROUNDWORKS"
    construction = "CONSTRUCTION"
    completed = "COMPLETE"

class ConstructionPhase(BaseModel):
    building_construction_phase: ConstructionPhaseEnum
    confidence_level: int
    reasoning: str
    
def analyze_construction_phase_openai(image_path: str) -> dict:
    try:
        # Check if file exists
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"Image file not found: {image_path}")
            
        with open(image_path, "rb") as image_file:
            try:
                # Attempt to read and encode the image
                image_data = image_file.read()
                encoded_image = base64.b64encode(image_data).decode()
                
                # Make API call
                completion = openai_client.beta.chat.completions.parse(
                    model="gpt-4o",
                    messages=[
                        {
                            "role": "system",
                            "content": """
You are an advanced satellite imagery analysis model specializing in construction phase classification. Your task is to determine the phase (GROUNDWORKS, CONSTRUCTION, COMPLETE) of the central building in an image. Follow a step-by-step reasoning process to ensure accuracy.

### Steps:
1. **Central Building Identification**:
   - Focus exclusively on the central building in the image area. Exclude adjacent or unrelated structures.

2. **Visual Feature Analysis**:
   - Identify and describe key features:
     - **Ground Features**: Cleared land, dirt, paving, landscaping.
     - **Building Structure**: Walls, roof, framework, exposed materials (e.g., concrete, steel).
     - **Construction Activity**: Machinery (cranes, scaffolding), temporary structures, active construction.
     - **Additional Indicators**: Amenities, parking areas, shadow patterns, or lighting effects.

3. **Phase Indicators**:
   - **GROUNDWORKS**:
     - Raw ground, excavation, heavy machinery for groundwork.
     - No visible foundation or structure.
   - **CONSTRUCTION**:
     - Structural framework, incomplete walls/roofing, construction machinery.
     - Building shape partially visible.
   - **COMPLETE**:
     - Completed roof and walls, landscaping, paved areas, amenities (e.g., pools, marked parking).
     - Absence of construction equipment or raw materials.

4. **Reasoning and Hypothesis**:
   - Log observations in structured JSON format:
     ```json
     {
         "ground_features": "cleared land",
         "building_structure": "incomplete roof and walls",
         "construction_activity": ["crane", "scaffolding"],
         "additional_indicators": "no landscaping"
     }
     ```
   - Based on the observations, create a hypothesis for the construction phase.

5. **Validation**:
   - Revisit the features to ensure consistency. Revise hypothesis if indicators conflict.

6. **Output**:
   - Return the construction phase, confidence level (0-100%), and detailed reasoning explaining the classification.
"""
                        },
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text", 
                                    "text": "Analyze this satellite image of a UAE construction site. Classify the construction phase and provide reasoning."
                                },
                                {
                                    "type": "image_url", 
                                    "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"}
                                }
                            ]
                        }
                    ],
                    response_format=ConstructionPhase,
                    temperature=0
                )
                result = completion.choices[0].message.parsed
                return {
                    'construction_phase': result.building_construction_phase.value,
                    'confidence_level': result.confidence_level,
                    'reasoning': result.reasoning
                }
                
            except base64.binascii.Error:
                raise ValueError("Invalid image data - could not encode to base64")
            except Exception as e:
                raise RuntimeError(f"Error during API call: {str(e)}")
                
    except Exception as e:
        print(f"Error analyzing construction phase: {str(e)}")
        raise


def analyze_construction_phase_gemini(image_path: str) -> dict:
    try:
        # Check if file exists
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"Image file not found: {image_path}")

        with open(image_path, "rb") as image_file:
            # Read and encode the image
            image_data = image_file.read()
            encoded_image = base64.b64encode(image_data).decode('utf-8')

        # Gemini prompt and API call
        prompt = """Analyze this satellite image of a UAE construction site. Classify the central building area as either groundworks (preparation phase), construction (building under construction), or complete (finished building with finishing touches)."""
        
        result = gemini_model.generate_content(
            [prompt, {'mime_type': 'image/jpeg', 'data': encoded_image}],
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=ConstructionPhase
            ),
        )
        
        # turn result from json to dict
        result_dict = json.loads(result.text)
        
        # return construction phase and confidence level
        return {
            'construction_phase': result_dict['building_construction_phase'],
            'confidence_level': result_dict['confidence_level']
        }

    except Exception as e:
        print(f"Error analyzing construction phase with Gemini: {str(e)}")
        raise