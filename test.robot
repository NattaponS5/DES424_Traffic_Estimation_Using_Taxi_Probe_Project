*** Settings ***
Library    SeleniumLibrary
Library    BuiltIn
Library    String 

*** Variables ***
${URL}      https://main.d2xz524t6c5f8p.amplifyapp.com/
${MAP_DIV}  id=map_a50e97e462c8de9c393b86f4b5d14f72
${TRAFFIC_LINE_CLASS}    leaflet-polyline 

*** Keywords ***
Open Website And Verify Map Loads
    [Documentation]    Open the website and verify the map loads successfully.
    Open Browser    ${URL}    Chrome
    Maximize Browser Window
    Wait Until Page Contains Element    ${MAP_DIV}    timeout=10s
    Element Should Be Visible    ${MAP_DIV}

Close Browser Safely
    [Documentation]    Close the browser safely after operations.
    Close Browser

*** Test Cases ***
Open Website And Verify Map Loads
    [Documentation]    Opens the website and verifies the map loads.
    Open Browser    ${URL}    Chrome
    Maximize Browser Window
    Wait Until Element Is Visible    ${MAP_DIV}    timeout=10s

Verify Zoom In and Out Using Mouse Scrolling
    [Documentation]    Check that zooming in and out with mouse scroll works.
    Open Website And Verify Map Loads
    Set Focus To Element    ${MAP_DIV}
    Repeat Keyword    5 Times    Execute JavaScript    window.scrollBy(0, -50)
    Sleep    1s
    Repeat Keyword    5 Times    Execute JavaScript    window.scrollBy(0, 50)
    Close Browser Safely

Verify Traffic Lines
    [Documentation]    Verifies that all traffic lines have a way ID and a stroke.
    Open Website And Verify Map Loads  # Your keyword to open and load the map
    ${traffic_lines}    Get WebElements    class=${TRAFFIC_LINE_CLASS}
    FOR    ${line}    IN    @{traffic_lines}
        ${way_id}    Get Element Attribute    ${line}    data-way-id
        Should Not Be Empty    ${way_id}
        ${stroke}    Get Element Attribute    ${line}    stroke
        Should Not Be Empty    ${stroke}
    END

Close Browser Successfully
    [Documentation]    Ensure the browser closes without errors.
    Open Website And Verify Map Loads
    Close Browser Safely