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

    fn build_tree(tokens: &Vec<String>) -> SearchTree {
        let mut stack: Vec<SearchTree> = Vec::new();
        let mut i = 0;

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
                stack.push(Self::build_tree(&sub_tokens));
                i = j;
            }
            else if token == "!" {
                let pop = stack.pop().unwrap();
                stack.push(SearchTree::Not(Box::new(pop)));
            }
            else if token == "|" {
                let left = stack.pop().unwrap();
                let right = Self::build_tree(&tokens[i+1..].to_vec());
                stack.push(SearchTree::Or(Box::new(left), Box::new(right)));
                break;
            }
            else {
                let mut trigrams: HashSet<String> = HashSet::default();
                crate::minute::Minute::explode(&mut trigrams, &token.to_string());
                stack.push(SearchTree::Token(
                    SearchToken {
                        token: token.to_string(),
                        trigrams,
                    }
                ));
            }
            i += 1;
        }

        if stack.len() == 0 {
            SearchTree::None
        }
        else if stack.len() == 1 {
            stack.pop().unwrap()
        }
        else {
            SearchTree::And(Box::new(stack.pop().unwrap()), Box::new(stack.pop().unwrap()))
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
                left.bloom_test(filter) || right.bloom_test(filter)
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

    pub fn tokens(&self) -> HashSet<String> {
        self.tree.list_trigrams()
    }

}


#[test]
fn test_tokenize_and_parse() {
    let fragments = SearchTree::tokenize(&"hello world".to_string());

    assert!(fragments.contains(&"hello".to_string()));
    assert!(fragments.contains(&"world".to_string()));

    let fragments = SearchTree::tokenize(&"hello \"world of tanks\"".to_string());

    assert!(fragments.contains(&"hello".to_string()));
    assert!(fragments.contains(&"world of tanks".to_string()));

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
    println!("{:?}", tree);

    assert!(tree.test(&"hello world of tanks"));
    assert!(!tree.test(&"hello sweet goodbye"));
    assert!(tree.test(&"goodbye sweet prince"));
    assert!(tree.test(&"sweet prince goodbye"));
    assert!(tree.test(&"sweet prince---09999 HELLOHLgoodbye=98282"));
    assert!(tree.test(&"sting stang stung h=hello t=world of tanks"));
}
