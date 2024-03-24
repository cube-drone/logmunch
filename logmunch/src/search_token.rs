use growable_bloom_filter::GrowableBloom;
use serde::{Serialize, Deserialize};
//use std::collections::HashSet;
use fxhash::FxHashSet as HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchToken{
    pub token: String,
    pub trigrams: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchTree{
    None,
    Token(SearchToken),
    Not(Box<SearchTree>),
    And(Box<SearchTree>, Box<SearchTree>),
    Or(Box<SearchTree>, Box<SearchTree>),
}
impl SearchTree {

    pub fn new(search_string: &str) -> Self {
        let fragments = Self::tokenize(search_string);
        Self::build_tree(&fragments)
    }

    fn tokenize(search_string: &str) -> Vec<String> {
        let mut tokens: Vec<String> = Vec::new();
        let mut current_token: Vec<char> = Vec::new();

        let mut escape = false;
        let mut in_quotes = false;
        for char in search_string.to_lowercase().chars() {
            if escape {
                current_token.push(char);
                escape = false;
            }
            else if in_quotes && char == '"' {
                // close quotes
                tokens.push(current_token.iter().collect());
                current_token = Vec::new();
                in_quotes = false;
            }
            else if current_token.len() == 0 && char == '"' {
                // open quotes
                in_quotes = true;
            }
            else if in_quotes{
                // inside quotes
                current_token.push(char);
            }
            else if current_token.len() == 0 && !escape && char == '(' {
                // open paren
                tokens.push("(".to_string());
            }
            else if !escape && char == ')'{
                // close paren
                tokens.push(")".to_string());
            }
            else if current_token.len() == 0 && !escape && char == '!' {
                // not
                tokens.push("!".to_string());
            }
            else if current_token.len() == 0 && !escape && char == '|' {
                // or
                tokens.push("|".to_string());
            }
            else if char == ' ' {
                if current_token.len() > 0 {
                    tokens.push(current_token.iter().collect());
                    current_token = Vec::new();
                }
                else{
                    // we don't do anything with whitespace outside of quotes
                }
            }
            else if char == '\\' {
                escape = true;
            }
            else{
                current_token.push(char);
            }
        }

        if current_token.len() > 0 {
            tokens.push(current_token.iter().collect());
        }

        tokens
    }

    fn quick_trigrams(token: &str) -> HashSet<String> {
        let mut trigrams: HashSet<String> = HashSet::default();
        crate::minute::Minute::explode(&mut trigrams, &token.to_string());
        trigrams
    }

    fn build_tree(tokens: &Vec<String>) -> SearchTree {
        Self::build_tree_int(tokens, false)
    }

    fn build_tree_int(tokens: &Vec<String>, pending_negation: bool) -> SearchTree {
        let mut stack: Vec<SearchTree> = Vec::new();
        let mut i = 0;
        let mut pending_negation = pending_negation;

        while i < tokens.len() {
            let token = &tokens[i];
            if token == "(" {
                let mut paren_count = 1;
                let mut j = i + 1;
                while j < tokens.len() {
                    if tokens[j] == "(" {
                        paren_count += 1;
                    }
                    else if tokens[j] == ")" {
                        paren_count -= 1;
                        if paren_count == 0 {
                            break;
                        }
                    }
                    j += 1;
                }
                let sub_tokens = tokens[i+1..j].to_vec();
                if pending_negation{
                    stack.push(SearchTree::Not(Box::new(Self::build_tree(&sub_tokens))));
                    pending_negation = false;
                }
                else{
                    stack.push(Self::build_tree(&sub_tokens));
                }
                i = j;
            }
            else if token == "!" {
                pending_negation = !pending_negation;
            }
            else if token == "|" && stack.len() > 0 {
                pending_negation = false;
                let left = stack.pop().unwrap();
                let right = Self::build_tree(&tokens[i+1..].to_vec());
                stack.push(SearchTree::Or(Box::new(left), Box::new(right)));
                break;
            }
            else if token == "|" && stack.len() == 0 {
                pending_negation = false;
                // that's weird, just ignore it
                continue;
            }
            else if token == "&" && stack.len() > 0 {
                pending_negation = false;
                let left = stack.pop().unwrap();
                let right = Self::build_tree(&tokens[i+1..].to_vec());
                stack.push(SearchTree::And(Box::new(left), Box::new(right)));
                break;
            }
            else if stack.len() == 1{
                let left = stack.pop().unwrap();
                let right = Self::build_tree_int(&tokens[i..].to_vec(), pending_negation);
                stack.push(SearchTree::And(Box::new(left), Box::new(right)));
                break;
            }
            else {
                if pending_negation{
                    stack.push(SearchTree::Not(Box::new(SearchTree::Token(
                        SearchToken {
                            token: token.to_string(),
                            trigrams: SearchTree::quick_trigrams(token),
                        }
                    ))));
                    pending_negation = false;
                }
                else{
                    stack.push(SearchTree::Token(
                        SearchToken {
                            token: token.to_string(),
                            trigrams: Self::quick_trigrams(token),
                        }
                    ));
                }
            }
            i += 1;
        }

        if stack.len() > 2 {
            panic!("The fuck?: {:?}", tokens);
        }
        else if stack.len() == 0 {
            SearchTree::None
        }
        else if stack.len() == 1 {
            stack.pop().unwrap()
        }
        else {
            if pending_negation {
                SearchTree::Not(Box::new(SearchTree::And(Box::new(stack.pop().unwrap()), Box::new(stack.pop().unwrap()))))
            }
            else {
                SearchTree::And(Box::new(stack.pop().unwrap()), Box::new(stack.pop().unwrap()))
            }
        }
    }

    pub fn list_trigrams(&self) -> HashSet<String> {
        match self {
            SearchTree::None => HashSet::default(),
            SearchTree::Token(token) => token.trigrams.clone(),
            SearchTree::Not(_tree) => HashSet::default(), // don't include trigrams from not
            SearchTree::And(left, right) => {
                let mut trigrams = left.list_trigrams();
                trigrams.extend(right.list_trigrams());
                trigrams
            },
            SearchTree::Or(left, right) => {
                let mut trigrams = left.list_trigrams();
                trigrams.extend(right.list_trigrams());
                trigrams
            }
        }
    }

    pub fn test(&self, event: &str) -> bool {
        match self {
            SearchTree::None => true,
            SearchTree::Token(token) => {
                // println!("Testing {} against {}", token.token, event);
                // check if the token is in the event
                event.to_lowercase().contains(&token.token)
            },
            SearchTree::Not(tree) => {
                !tree.test(event)
            },
            SearchTree::And(left, right) => {
                left.test(event) && right.test(event)
            },
            SearchTree::Or(left, right) => {
                if left.as_ref() == &SearchTree::None {
                    return right.test(event);
                }
                if right.as_ref() == &SearchTree::None {
                    return left.test(event);
                }
                left.test(event) || right.test(event)
            }
        }
    }

    pub fn bloom_test(&self, filter: &GrowableBloom) -> bool {
        match self {
            SearchTree::None => true,
            SearchTree::Token(token) => {
                for trigram in token.trigrams.iter() {
                    if !filter.contains(trigram) {
                        return false;
                    }
                }
                return true;
            }
            SearchTree::Not(_tree) => true,
            SearchTree::And(left, right) => {
                left.bloom_test(filter) && right.bloom_test(filter)
            },
            SearchTree::Or(left, right) => {
                if left.as_ref() == &SearchTree::None {
                    return right.bloom_test(filter);
                }
                if right.as_ref() == &SearchTree::None {
                    return left.bloom_test(filter);
                }
                left.bloom_test(filter) || right.bloom_test(filter)
            }
        }
    }

    ///
    /// We'll give you a lambda function that takes a HashSet of trigrams
    /// and returns a boolean. The lambda function should return true if the data set
    /// contains all of the trigrams in the hashset.
    ///
    pub fn lambda_test(&self, lambda: &dyn Fn(&HashSet<String>) -> bool) -> bool {
        match self {
            SearchTree::None => true,
            SearchTree::Token(token) => {
                lambda(&token.trigrams)
            },
            SearchTree::Not(_tree) => {
                // we should just ignore the tree here
                //  because the presence of trigrams, say, "wri", "tab", "ble"
                //  doesn't necessarily mean that the event contains "writable"
                //  it could be "tably wribble"
                //  so !writable should still search against nodes that contain "wri", "tab", "ble"
                // (we do the same thing with bloom filters, above)
                true
            },
            SearchTree::And(left, right) => {
                left.lambda_test(lambda) && right.lambda_test(lambda)
            },
            SearchTree::Or(left, right) => {
                if left.as_ref() == &SearchTree::None {
                    return right.lambda_test(lambda);
                }
                if right.as_ref() == &SearchTree::None {
                    return left.lambda_test(lambda);
                }
                left.lambda_test(lambda) || right.lambda_test(lambda)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Search{
    pub search_string: String,
    pub tree: SearchTree
}

impl Search{
    pub fn new(search_string: &str) -> Self {
        Search {
            search_string: search_string.to_string(),
            tree: SearchTree::new(search_string)
        }
    }

    pub fn test(&self, event: &str) -> bool {
        self.tree.test(event)
    }

    pub fn lambda_test(&self, lambda: &dyn Fn(&HashSet<String>) -> bool) -> bool {
        self.tree.lambda_test(lambda)
    }

    pub fn tokens(&self) -> HashSet<String> {
        self.tree.list_trigrams()
    }

    pub fn search_string(&self) -> String {
        self.search_string.clone()
    }

    pub fn tree(&self) -> SearchTree {
        self.tree.clone()
    }

}

#[test]
fn test_tokenize_and_parse() {
    let fragments = SearchTree::tokenize(&"hello world".to_string());

    assert!(fragments.contains(&"hello".to_string()));
    assert!(fragments.contains(&"world".to_string()));

    let tree = SearchTree::build_tree(&fragments);

    assert_eq!(tree,
        SearchTree::And(
            Box::new(SearchTree::Token(SearchToken {
                token: "hello".to_string(),
                trigrams: SearchTree::quick_trigrams("hello")
            })),
            Box::new(SearchTree::Token(SearchToken {
                token: "world".to_string(),
                trigrams: SearchTree::quick_trigrams("world")
            }))
        )
    );

    let fragments = SearchTree::tokenize(&"hello \"world of tanks\"".to_string());

    assert!(fragments.contains(&"hello".to_string()));
    assert!(fragments.contains(&"world of tanks".to_string()));

    let tree = SearchTree::build_tree(&fragments);

    assert_eq!(tree,
        SearchTree::And(
            Box::new(SearchTree::Token(SearchToken {
                token: "hello".to_string(),
                trigrams: SearchTree::quick_trigrams("hello")
            })),
            Box::new(SearchTree::Token(SearchToken {
                token: "world of tanks".to_string(),
                trigrams: SearchTree::quick_trigrams("world of tanks")
            }))
        )
    );

    let fragments = SearchTree::tokenize(&"(hello \"world of tanks\") | (goodbye \"sweet prince\")".to_string());

    assert_eq!(fragments, vec![
        "(".to_string(),
        "hello".to_string(),
        "world of tanks".to_string(),
        ")".to_string(),
        "|".to_string(),
        "(".to_string(),
        "goodbye".to_string(),
        "sweet prince".to_string(),
        ")".to_string()]);

    let tree = SearchTree::build_tree(&fragments);

    assert_eq!(tree,
        SearchTree::Or(
            Box::new(SearchTree::And(
                Box::new(SearchTree::Token(SearchToken {
                    token: "hello".to_string(),
                    trigrams: SearchTree::quick_trigrams("hello")
                })),
                Box::new(SearchTree::Token(SearchToken {
                    token: "world of tanks".to_string(),
                    trigrams: SearchTree::quick_trigrams("world of tanks")
                }))
            )),
            Box::new(SearchTree::And(
                Box::new(SearchTree::Token(SearchToken {
                    token: "goodbye".to_string(),
                    trigrams: SearchTree::quick_trigrams("goodbye")
                })),
                Box::new(SearchTree::Token(SearchToken {
                    token: "sweet prince".to_string(),
                    trigrams: SearchTree::quick_trigrams("sweet prince")
                }))
            ))
        )
    );

    assert!(tree.test(&"hello world of tanks"));
    assert!(!tree.test(&"hello sweet goodbye"));
    assert!(tree.test(&"goodbye sweet prince"));
    assert!(tree.test(&"sweet prince goodbye"));
    assert!(tree.test(&"sweet prince---09999 HELLOHLgoodbye=98282"));
    assert!(tree.test(&"sting stang stung h=hello t=world of tanks"));
}

#[test]
fn test_negation() {
    let fragments = SearchTree::tokenize(&"!hello".to_string());
    let tree = SearchTree::build_tree(&fragments);

    assert!(!tree.test(&"hello world"));
    assert!(tree.test(&"goodbye world"));

    let fragments = SearchTree::tokenize(&"!hello | goodbye".to_string());
    let tree = SearchTree::build_tree(&fragments);
    assert!(!tree.test(&"hello world"));
    assert!(tree.test(&"goodbye world"));

    let fragments = SearchTree::tokenize(&"!hello & !goodbye".to_string());
    let tree = SearchTree::build_tree(&fragments);

    assert_eq!(tree,
        SearchTree::And(
            Box::new(SearchTree::Not(Box::new(SearchTree::Token(SearchToken {
                token: "hello".to_string(),
                trigrams: SearchTree::quick_trigrams("hello")
            })))),
            Box::new(SearchTree::Not(Box::new(SearchTree::Token(SearchToken {
                token: "goodbye".to_string(),
                trigrams: SearchTree::quick_trigrams("goodbye")
            }))))
        )
    );

    assert!(!tree.test(&"hello world"));
    assert!(!tree.test(&"goodbye world"));

    assert!(!tree.test(&"hello goodbye"));
    assert!(!tree.test(&"mellow hello how are you feeling goodbye toby"));
    assert!(tree.test(&"mellow how are you feeling toby"));

    let fragments = SearchTree::tokenize(&"presence !homer".to_string());
    assert_eq!(fragments, vec!["presence".to_string(), "!".to_string(), "homer".to_string()]);

    let tree = SearchTree::build_tree(&fragments);

    assert_eq!(tree,
        SearchTree::And(
            Box::new(SearchTree::Token(SearchToken {
                token: "presence".to_string(),
                trigrams: SearchTree::quick_trigrams("presence")
            })),
            Box::new(SearchTree::Not(Box::new(SearchTree::Token(SearchToken {
                token: "homer".to_string(),
                trigrams: SearchTree::quick_trigrams("homer")
            }))))
        )
    );
}

#[test]
fn test_negation_more(){
    let search = Search::new("presence !homer");

    assert!(!search.test(&"2023-11-10T04:53:04.096624+00:00 girlboss 09c01c523eef 300704 -  212.102.46.118 - - [10/Nov/2023:04:53:04 +0000] \"POST /homer-man-x/presence/update HTTP/1.1\""));
    assert!(search.test(&"2023-11-10T04:53:04.096624+00:00 girlboss 09c01c523eef 300704 -  212.102.46.118 - - [10/Nov/2023:04:53:04 +0000] \"POST /presence/update HTTP/1.1\""));

    let search = Search::new("hats !bats !cats !rats mats");

    assert!(search.test(&"mats hats mats"));
    assert!(search.test(&"hats mats hats"));
    assert!(!search.test(&"hats cats hats"));
    assert!(!search.test(&"hats bats hats"));
    assert!(!search.test(&"hats rats hats"));

    let search = Search::new("!bats !cats hats mats !rats");

    assert!(search.test(&"mats hats mats"));
    assert!(search.test(&"hats mats hats"));
    assert!(!search.test(&"hats cats hats"));
    assert!(!search.test(&"hats bats hats"));
    assert!(!search.test(&"hats rats hats"));
}