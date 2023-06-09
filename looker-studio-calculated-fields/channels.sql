case
when session_source = '(direct)' and (session_medium in ('(not set)','(none)')) then 'Direct'
        when regexp_contains(session_campaign, 'cross-network') then 'Cross-network'
        when (regexp_contains(session_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
            or regexp_contains(session_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$'))
            and regexp_contains(session_medium, '^(.*cp.*|ppc|paid.*)$') then 'Paid Shopping'
        when regexp_contains(session_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
            and regexp_contains(session_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Search'
        when regexp_contains(session_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
            and regexp_contains(session_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Social'
        when regexp_contains(session_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
            and regexp_contains(session_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Video'
        when session_medium in ('display', 'banner', 'expandable', 'interstitial', 'cpm') then 'Display'
        when regexp_contains(session_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
            or regexp_contains(session_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
        when regexp_contains(session_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
            or session_medium in ('social','social-network','social-media','sm','social network','social media') then 'Organic Social'
        when regexp_contains(session_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
            or regexp_contains(session_medium,'^(.*video.*)$') then 'Organic Video'
        when regexp_contains(session_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
            or session_medium = 'organic' then 'Organic Search'
        when regexp_contains(session_source,'email|e-mail|e_mail|e mail')
            or regexp_contains(session_medium,'email|e-mail|e_mail|e mail') then 'Email'
        when session_medium = 'affiliate' then 'Affiliates'
        when session_medium = 'referral' then 'Referral'
        when session_medium = 'audio' then 'Audio'
        when session_medium = 'sms' then 'SMS'
        when REGEXP_CONTAINS(session_medium, 'push$')
            or regexp_contains(session_medium,'mobile|notification') then 'Mobile Push Notifications'
        else 'Unassigned'
    end
