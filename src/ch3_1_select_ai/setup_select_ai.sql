-- ============================================================
-- Chapter 3.1: Select AI in Autonomous Database
-- ============================================================
-- NOTE: Select AI requires Oracle Autonomous Database (cloud).
-- These scripts will NOT work on a local Oracle Free container.
-- They are provided as reference for ADB deployments.
-- ============================================================

-- Step 1: Create credential for OpenAI API access
BEGIN
  DBMS_CLOUD.CREATE_CREDENTIAL(
    credential_name => 'OPENAI_CRED',
    username        => 'openai',
    password        => 'sk-your-openai-api-key-here'
  );
END;
/

-- Step 2: Create AI profile with restricted schema access
BEGIN
  DBMS_CLOUD_AI.CREATE_PROFILE(
    profile_name => 'SALES_ANALYST_PROFILE',
    attributes   => JSON_OBJECT(
      'provider'        VALUE 'openai',
      'credential_name' VALUE 'OPENAI_CRED',
      'model'           VALUE 'gpt-4',
      'object_list'     VALUE JSON_ARRAY(
        JSON_OBJECT('owner' VALUE 'SALES', 'name' VALUE 'CUSTOMERS'),
        JSON_OBJECT('owner' VALUE 'SALES', 'name' VALUE 'ORDERS'),
        JSON_OBJECT('owner' VALUE 'SALES', 'name' VALUE 'ORDER_ITEMS'),
        JSON_OBJECT('owner' VALUE 'SALES', 'name' VALUE 'PRODUCTS')
      )
    )
  );
END;
/

-- Step 3: Activate profile for current session
BEGIN
  DBMS_CLOUD_AI.SET_PROFILE(
    profile_name => 'SALES_ANALYST_PROFILE'
  );
END;
/

-- Step 4: Use natural language queries
-- Execute directly:
SELECT AI 'show me the top 10 customers by revenue in 2024';

-- Show generated SQL without executing:
SELECT AI SHOWSQL 'show me the top 10 customers by revenue in 2024';
