pub trait AllElementsSame {
    type Element;
    fn all_elements_same(&mut self) -> Option<Self::Element>;
}

impl<I, T> AllElementsSame for I
where
    I: Iterator<Item = T>,
    T: std::cmp::PartialEq,
{
    type Element = T;

    fn all_elements_same(&mut self) -> Option<Self::Element> {
        let element = self.next()?;
        if self.all(|e| e == element) {
            Some(element)
        } else {
            None
        }
    }
}
